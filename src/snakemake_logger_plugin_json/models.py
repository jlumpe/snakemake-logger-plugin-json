"""Classes used to represent log records."""

from datetime import datetime
import logging
from typing import Any, Callable, NoReturn, Self, TypeVar, ClassVar, Literal, TypeAlias
from uuid import UUID
from dataclasses import dataclass, field, fields, MISSING
import time
from pathlib import PurePath

from pydantic import TypeAdapter, ConfigDict, model_serializer, SerializerFunctionWrapHandler
from snakemake_interface_logger_plugins.common import LogEvent


T = TypeVar('T')
_T_modeltype = TypeVar('_T_modeltype', bound=type['JsonLogRecord'])


#: Describes the general category of log record.
RecordType: TypeAlias = Literal['standard', 'meta', 'snakemake']


NAMED_LEVELS = (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)


def make_registration_decorator(
	registry: dict[Any, Any],
	attrname: str,
) -> Callable[[_T_modeltype], _T_modeltype]:

	def register(cls: _T_modeltype) -> _T_modeltype:
		key = getattr(cls, attrname)
		if key in registry:
			raise ValueError(f'Model already registered for key {key!r}')
		registry[key] = cls
		return cls

	return register


class TypeAdapterCache:
	"""Caches Pydantic TypeAdapters.

	Enables using Pydantic to validate non-BaseModel types (including dataclasses), but avoids
	creating a new ``TypeAdapter`` instance each time.
	"""

	cache: dict[type, TypeAdapter]

	def __init__(self):
		self.cache = dict()

	def get(self, typ: type) -> TypeAdapter:
		if typ in self.cache:
			return self.cache[typ]
		adapter = TypeAdapter(typ)
		self.cache[typ] = adapter
		return adapter

	def validate_python(self, typ: type[T], value, **kw) -> T:
		adapter = self.get(typ)
		return adapter.validate_python(value, **kw)

	def validate_json(self, typ: type[T], data: str | bytes | bytearray, **kw) -> T:
		adapter = self.get(typ)
		return adapter.validate_json(data, **kw)

	def dump_python(self, value, astype: type | None = None, **kw) -> Any:
		if astype is None:
			astype = type(value)
		adapter = self.get(astype)
		return adapter.dump_python(value, **kw)

	def dump_json(self, value, astype: type | None = None, **kw) -> bytes:
		if astype is None:
			astype = type(value)
		adapter = self.get(astype)
		return adapter.dump_json(value, **kw)


adapter_cache = TypeAdapterCache()


# ------------------------------------------------------------------------------------------------ #
#                                         Non-record models                                        #
# ------------------------------------------------------------------------------------------------ #

@dataclass
class ExceptionInfo:
	"""Information from a caught exception."""

	message: str
	type: str

	@staticmethod
	def from_exception(exc: BaseException) -> 'ExceptionInfo':
		typ = type(exc)
		typestr = type.__qualname__
		if typ.__module__ not in (None, 'builtins'):
			typestr = f'{typ.__module__}.{typestr}'
		return ExceptionInfo(message=str(exc), type=typestr)


# ------------------------------------------------------------------------------------------------ #
#                                               Base                                               #
# ------------------------------------------------------------------------------------------------ #

@dataclass(kw_only=True)
class JsonLogRecord:
	"""Base class for models of a JSON-formatted log records.

	Can be constructed from builtin :class:`logging.LogRecord` instances using the
	:meth:`from_builtin` method. Afterwards should be able to be converted to/from JSON losslessly.

	Attributes
	----------
	type
		String describing the general category of log record (class attribute).
	message
		Formatted log message.
	levelno
		Numeric level.
	created
		Timestamp when log record was created.
	"""

	# __pydantic_config__: ClassVar = ConfigDict(extra='forbid')

	type: ClassVar[RecordType]

	message: str | None
	levelno: int
	# This is how the Python documentation says LogRecord.created is set, unsure how it's supposed
	# to be different than time.time()
	# https://docs.python.org/3/library/logging.html#logrecord-attributes
	created: float = field(default_factory=lambda: time.time_ns() / 1e9)
	exc_info: ExceptionInfo | None = None

	def __init__(self, *args, **kw):
		# This will be overwritten in any child classes with a @dataclass decorator
		raise TypeError(f"Can't instantiate abstract class {self.__class__.__name__}")

	@property
	def created_dt(self) -> datetime:
		"""Created timestamp as a :class:`datetime.datetime` instance."""
		return datetime.fromtimestamp(self.created)

	@property
	def levelname(self) -> str:
		"""String associated with numeric log level.

		This is always determined from :attr:`levelno`, so no need to store as an actual attribute.
		"""
		return logging.getLevelName(self.levelno)

	@staticmethod
	def from_builtin(record: logging.LogRecord) -> 'JsonLogRecord':
		"""Construct a log record model from a builtin :class:`logging.LogRecord` instance.

		Parameters
		----------
		record
			Log record instance from the standard logging system.

		Returns
		-------
		JsonLogRecord
			An instance of a suitable subclass of :class:`JsonLogRecord`.
		"""
		event = getattr(record, 'event', None)
		if isinstance(event, LogEvent):
			cls = SNAKEMAKE_RECORD_MODELS[event]
			return cls._from_builtin(record)
		else:
			return StandardLogRecord._from_builtin(record)

	@classmethod
	def _from_builtin(cls, record: logging.LogRecord) -> Self:
		"""This is specialized to the particular subclass.

		Assumes the passed record matches the class.
		"""
		attrs = cls._get_attrs(record)
		# return cls(**attrs)
		return adapter_cache.validate_python(cls, attrs)

	@classmethod
	def _get_attrs(cls, record: logging.LogRecord) -> dict[str, Any]:
		"""Get attribute values from builtin log record."""
		attrs: dict[str, Any] = dict(
			message=record.message,
			levelno=record.levelno,
			created=record.created,
		)
		if record.exc_info is not None:
			typ, exc, tb = record.exc_info
			if exc is not None:
				attrs['exc_info'] = ExceptionInfo.from_exception(exc)
		return attrs

	@model_serializer(mode='wrap')
	def _serialize(self, handler: SerializerFunctionWrapHandler):
		# Set these first so they appear at the of the record in multiline format, for easier reading
		d: dict[str, Any] = dict(type=self.type)
		if hasattr(self, 'event'):
			d['event'] = str(self.event)
		# Add this just for human readability
		d['levelname'] = logging.getLevelName(self.levelno) if self.levelno in NAMED_LEVELS else None
		d |= handler(self)
		if d['exc_info'] is None:
			del d['exc_info']
		return d


@dataclass(kw_only=True)
class StandardLogRecord(JsonLogRecord):
	"""A standard Python log record."""

	type = 'standard'


# ------------------------------------------------------------------------------------------------ #
#                                               Meta                                               #
# ------------------------------------------------------------------------------------------------ #

#: Mapping from meta log event types to model classes.
META_RECORD_MODELS: dict[str, type['MetaLogRecord']] = dict()

register_meta_model = make_registration_decorator(META_RECORD_MODELS, 'event')


@dataclass(kw_only=True, init=False)
class MetaLogRecord(JsonLogRecord):
	"""Log record containing information about the logging session itself.

	Is not created from a :class:`logging.LogRecord` instance.

	Attributes
	----------
	event
		String describing the meta log event (class attribute).
	"""

	type = 'meta'
	event: ClassVar[str]

	@staticmethod
	def _builtin_error() -> NoReturn:
		raise TypeError('MetaLogRecord subclasses cannot be constructed from builtin log records')

	@classmethod
	def _from_builtin(cls, record: logging.LogRecord):
		cls._builtin_error()

	@classmethod
	def _get_attrs(cls, record: logging.LogRecord):
		cls._builtin_error()


@register_meta_model
@dataclass(kw_only=True)
class LoggingStartedRecord(MetaLogRecord):
	"""Indicates the initialization of the logging system.

	Attributes
	----------
	pid
		ID of snakemake process. Can be used to check whether the process is still running.
	proc_started
		Timestamp when the snakemake process started, if available. Can be used in addition to PID
		to avoid edge case of PID reuse.
	"""

	event = 'logging_started'

	pid: int
	proc_started: float | None = None
	levelno: int = logging.INFO
	message: str | None = 'JSON logging plugin initialized'


@register_meta_model
@dataclass(kw_only=True)
class FormattingErrorRecord(MetaLogRecord):
	"""Indicates an error formatting a log record.

	Attributes
	----------
	record_partial
		Dictionary of attributes that were successfully extracted from the log record.
	exception
		Information on the exception that occurred during formatting, if available.
	"""

	event = 'formatting_error'

	record_partial: dict[str, Any]
	exception: ExceptionInfo | None = None
	levelno: int = logging.ERROR
	message: str | None = 'Error converting log record to JSON'

	@classmethod
	def create(
		cls,
		record: logging.LogRecord,
		exception: BaseException | ExceptionInfo | None = None,
		message: str | None = None
	) -> 'FormattingErrorRecord':
		"""Create from the record being formatted and the exception that occurred.

		Parameters
		----------
		record
			The log record that could not be formatted.
		exception
			The exception that occurred during formatting, if available.
		message
			Message to use. Defaults to exception message if given.
		"""

		partial = cls._extract_partial(record)

		if isinstance(exception, BaseException):
			exception = ExceptionInfo.from_exception(exception)

		if message is None:
			if exception is not None:
				message = exception.message
			else:
				message = 'Error formatting log record'

		return cls(
			message=message,
			record_partial=partial,
			exception=exception,
		)

	@staticmethod
	def _extract_partial(record: logging.LogRecord) -> dict[str, Any]:
		attrs = {}
		for field in fields(StandardLogRecord):
			if hasattr(record, field.name):
				attrs[field.name] = getattr(record, field.name)
		return attrs


# ------------------------------------------------------------------------------------------------ #
#                                         Snakemake events                                         #
# ------------------------------------------------------------------------------------------------ #

#: Mapping from Snakemake log event types to model classes.
SNAKEMAKE_RECORD_MODELS: dict[LogEvent, type["SnakemakeLogRecord"]] = dict()

register_snakemake_model = make_registration_decorator(SNAKEMAKE_RECORD_MODELS, 'event')


class SnakemakeLogRecord(JsonLogRecord):
	"""Base class for a Snakemake log record model.

	Attributes
	----------
	event
		The Snakemake log event type (class attribute).
	"""

	type = 'snakemake'
	event: ClassVar[LogEvent]

	@classmethod
	def _get_attrs(cls, record: logging.LogRecord) -> dict[str, Any]:
		attrs = super()._get_attrs(record)

		for field in fields(cls):
			if field.name not in attrs:
				if hasattr(record, field.name):
					attrs[field.name] = getattr(record, field.name)
				elif field.default is MISSING and field.default_factory is MISSING:
					raise AttributeError(f'LogRecord with event type {cls.event} missing required attribute {field.name!r}')

		return attrs

	def associated_jobs(self) -> list[int]:
		"""Get any job IDs this record is associated with."""
		return []


@register_snakemake_model
@dataclass(kw_only=True)
class ErrorRecord(SnakemakeLogRecord):
	event = LogEvent.ERROR

	exception: str | None = None
	location: str | None = None
	rule: str | None = None
	trackeback: str | None = None
	file: str | None = None
	line: str | None = None


@register_snakemake_model
@dataclass(kw_only=True)
class WorkflowStartedRecord(SnakemakeLogRecord):
	event = LogEvent.WORKFLOW_STARTED

	workflow_id: UUID
	# snakefile: str | None
	snakefile: PurePath | None


@register_snakemake_model
@dataclass(kw_only=True)
class JobInfoRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_INFO

	jobid: int
	rule_name: str
	threads: int
	input: list[str] | None = None
	output: list[str] | None = None
	log: list[str] | None = None
	benchmark: str | None = None
	rule_msg: str | None = None
	wildcards: dict[str, Any] | None = None
	reason: str | None = None
	shellcmd: str | None = None
	priority: int | None = None
	# resources: dict[str, Any] | None = None
	resources: dict[str, Any] | list[Any] | None = None

	def associated_jobs(self) -> list[int]:
		return [self.jobid]


@register_snakemake_model
@dataclass(kw_only=True)
class JobStartedRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_STARTED

	# job_ids: list[int]
	jobs: list[int]

	def associated_jobs(self) -> list[int]:
		return self.jobs


@register_snakemake_model
@dataclass(kw_only=True)
class JobFinishedRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_FINISHED

	job_id: int

	def associated_jobs(self) -> list[int]:
		return [self.job_id]


@register_snakemake_model
@dataclass(kw_only=True)
class ShellCmdRecord(SnakemakeLogRecord):
	event = LogEvent.SHELLCMD

	jobid: int | None = None
	shellcmd: str | None = None
	rule_name: str | None = None

	def associated_jobs(self) -> list[int]:
		return [] if self.jobid is None else [self.jobid]


@register_snakemake_model
@dataclass(kw_only=True)
class JobErrorRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_ERROR

	jobid: int

	def associated_jobs(self) -> list[int]:
		return [self.jobid]


@register_snakemake_model
@dataclass(kw_only=True)
class GroupInfoRecord(SnakemakeLogRecord):
	event = LogEvent.GROUP_INFO

	group_id: int
	jobs: list[Any]

	def associated_jobs(self) -> list[int]:
		return self.jobs


@register_snakemake_model
@dataclass(kw_only=True)
class GroupErrorRecord(SnakemakeLogRecord):
	event = LogEvent.GROUP_ERROR

	groupid: int
	aux_logs: list[Any]
	job_error_info: dict[str, Any]


@register_snakemake_model
@dataclass(kw_only=True)
class ResourcesInfoRecord(SnakemakeLogRecord):
	event = LogEvent.RESOURCES_INFO

	nodes: list[str] | None = None
	cores: int | None = None
	provided_resources: dict[str, Any] | None = None


@register_snakemake_model
@dataclass(kw_only=True)
class DebugDagRecord(SnakemakeLogRecord):
	event = LogEvent.DEBUG_DAG

	status: str | None = None
	job: Any = None
	file: str | None = None
	exception: str | None = None


@register_snakemake_model
@dataclass(kw_only=True)
class ProgressRecord(SnakemakeLogRecord):
	event = LogEvent.PROGRESS

	done: int
	total: int


@register_snakemake_model
@dataclass(kw_only=True)
class RulegraphRecord(SnakemakeLogRecord):
	event = LogEvent.RULEGRAPH

	rulegraph: dict[str | None, Any] | None = None


@register_snakemake_model
@dataclass(kw_only=True)
class RunInfoRecord(SnakemakeLogRecord):
	event = LogEvent.RUN_INFO

	# per_rule_job_counts: dict[str, int]
	# total_job_counts: int
	stats: dict[str, int]


# ------------------------------------------------------------------------------------------------ #
#                                               Misc                                               #
# ------------------------------------------------------------------------------------------------ #

#: All non-abstract model classes
ALL_MODELS: list[type[JsonLogRecord]] = [
	StandardLogRecord,
	*META_RECORD_MODELS.values(),
	*SNAKEMAKE_RECORD_MODELS.values(),
]
