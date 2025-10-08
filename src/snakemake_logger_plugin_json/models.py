"""Classes used to represent log records."""

from datetime import datetime
import logging
from typing import Any, Callable, Self, TypeVar, ClassVar, Literal, TypeAlias
from uuid import UUID
from dataclasses import dataclass, field, fields, MISSING
import time
from pathlib import PurePath

from snakemake_interface_logger_plugins.common import LogEvent


_T_modeltype = TypeVar('_T_modeltype', bound=type['JsonLogRecord'])


#: Describes the general category of log record.
RecordType: TypeAlias = Literal['standard', 'meta', 'snakemake']


def make_registration_decorator(
	registry: dict[Any, Any],
	attrname: str,
) -> Callable[[_T_modeltype], _T_modeltype]:

	def register(cls: _T_modeltype) -> _T_modeltype:
		key = getattr(cls, attrname)
		if key in registry:
			raise ValueError(f'Model already reigstered for key {key!r}')
		registry[key] = cls
		return cls

	return register


# ------------------------------------------------------------------------------------------------ #
#                                         Non-record models                                        #
# ------------------------------------------------------------------------------------------------ #

@dataclass
class ExceptionInfo:
	"""Information from a caught exception."""

	message: str
	type: str

	def from_exception(exc: BaseException) -> 'ExceptionInfo':
		typ = type(exc)
		return ExceptionInfo(
			message=str(exc),
			type=f'{typ.__module__}.{typ.__name__}',
		)


# ------------------------------------------------------------------------------------------------ #
#                                               Base                                               #
# ------------------------------------------------------------------------------------------------ #

@dataclass(kw_only=True)
class JsonLogRecord:
	"""Base class for models of a JSON-formatted log records.

	These are primarily used for parsing records from JSON output, but you can also construct from
	builtin :class:`logging.LogRecord` instances using the :meth:`from_builtin` method.

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
	levelname
	    String associated with numeric level, if any.
	"""

	type: ClassVar[RecordType]

	message: str | None
	levelno: int
	levelname: str | None = None
	# This is how the Python documentation says LogRecord.created is set, unsure how it's supposed
	# to be different than time.time()
	# https://docs.python.org/3/library/logging.html#logrecord-attributes
	created: float = field(default_factory=lambda: time.time_ns() / 1e9)

	def __init__(self, *args, **kw):
		# This will be overwritten in any child classes with a @dataclass decorator
		raise TypeError(f"Can't instantiate abstract class {self.__class__.__name__}")

	@property
	def created_dt(self) -> datetime:
		return datetime.fromtimestamp(self.created)

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
		if hasattr(record, 'event') and isinstance(record.event, LogEvent):
			cls = SNAKEMAKE_RECORD_MODELS[record.event]
			return cls._from_builtin(record)
		else:
			return StandardLogRecord._from_builtin(record)

	@classmethod
	def _from_builtin(cls, record: logging.LogRecord) -> Self:
		"""This is specialized to the particular subclass.

		Assumes the passed record matches the class.
		"""
		attrs = cls._get_attrs(record)
		return cls(**attrs)

	@classmethod
	def _get_attrs(cls, record: logging.LogRecord) -> dict[str, Any]:
		"""Get attribute values from builtin log record."""
		return dict(
			message=record.message,
			levelno=record.levelno,
			levelname=record.levelname,
			created=record.created,
		)


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

	levelno: int = 0

	@staticmethod
	def _builtin_error():
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
	message: str = 'JSON logging plugin initialized'


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

	@classmethod
	def create(
		cls,
		record: logging.LogRecord,
		exception: Exception | ExceptionInfo | None = None,
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

		if isinstance(exception, Exception):
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
	benchmark: list[str] | None = None
	rule_msg: str | None = None
	wildcards: dict[str, Any] | None = None
	reason: str | None = None
	shellcmd: str | None = None
	priority: int | None = None
	# resources: dict[str, Any] | None = None
	resources: dict[str, Any] | list[Any] | None = None


@register_snakemake_model
@dataclass(kw_only=True)
class JobStartedRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_STARTED

	# job_ids: list[int]
	jobs: list[int]


@register_snakemake_model
@dataclass(kw_only=True)
class JobFinishedRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_FINISHED

	job_id: int


@register_snakemake_model
@dataclass(kw_only=True)
class ShellCmdRecord(SnakemakeLogRecord):
	event = LogEvent.SHELLCMD

	jobid: int | None = None
	shellcmd: str | None = None
	rule_name: str | None = None


@register_snakemake_model
@dataclass(kw_only=True)
class JobErrorRecord(SnakemakeLogRecord):
	event = LogEvent.JOB_ERROR

	jobid: int


@register_snakemake_model
@dataclass(kw_only=True)
class GroupInfoRecord(SnakemakeLogRecord):
	event = LogEvent.GROUP_INFO

	group_id: int
	jobs: list[Any]


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
