from typing import Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
import os

from pydantic import TypeAdapter
from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

from .models import JsonLogRecord, FormattingErrorRecord, LoggingStartedRecord, adapter_cache


def make_logfile_path(workdir: os.PathLike | None = None, timestamp: datetime | None = None) -> str:
	"""Default log file path."""

	if timestamp is None:
		timestamp = datetime.now()

	filename = datetime.now().isoformat().replace(':', '') + '.log'

	path = os.path.join('.snakemake/log/json', filename)
	if workdir is not None:
		path = os.path.join(workdir, path)

	return path


@dataclass
class JsonLogHandlerSettings(LogHandlerSettingsBase):
	file: Optional[str] = field(default=None, metadata={
		'help': 'File to write to (or - to use stderr).',
	})
	multiline: bool = field(default=False, metadata={
		'help': 'Write records in indented multi-line format.',
	})
	rulegraph: bool = field(default=False, metadata={
		'help': 'Output rule graph.',
	})
	validate: bool = field(default=False, metadata={
		'help': 'Validate log record attributes before writing (for testing).',
	})


@dataclass
class JsonFormatter:
	"""Log formatter.

	Attributes
	----------
	multiline
		Write each record over multiple lines with indentation and nice formatting. Easier for a
		human to read but harder to parse. The alternative is JSONL format.
	validate
		Validate the record's attributes.
	"""

	multiline: bool = False
	validate: bool = False

	def format(self, record: logging.LogRecord | JsonLogRecord) -> str:
		json_record = self._get_json_record(record)
		return self._format_json_record(json_record)

	def _get_json_record(self, record: logging.LogRecord | JsonLogRecord) -> JsonLogRecord:
		if isinstance(record, JsonLogRecord):
			return record

		try:
			json_record = JsonLogRecord.from_builtin(record)
			if self.validate:
				adapter = TypeAdapter(type(json_record))
				adapter.validate_python(json_record.__dict__)
			return json_record

		except Exception as exc:
			return self._make_error_record(record, exc)

	def _format_json_record(self, json_record: JsonLogRecord) -> str:
		data = adapter_cache.dump_json(json_record, indent=2 if self.multiline else None)
		return data.decode()

	def _make_error_record(self, record: logging.LogRecord, exc: Exception) -> FormattingErrorRecord:
		return FormattingErrorRecord.create(record, exc)


class JsonLogHandler(LogHandlerBase):

	settings: JsonLogHandlerSettings
	baseFilename: str | None
	handler: logging.Handler

	def __init__(self, *args):
		logging.Handler.__init__(self)
		LogHandlerBase.__init__(self, *args)

	def __post_init__(self) -> None:
		if self.settings.file == '-':
			self.baseFilename = None
			self.handler = logging.StreamHandler()

		else:
			if self.settings.file:
				self.baseFilename = self.settings.file
			else:
				self.baseFilename = make_logfile_path()
				os.makedirs(os.path.dirname(self.baseFilename), exist_ok=True)

			self.handler = logging.FileHandler(self.baseFilename, mode='w')

		formatter = JsonFormatter(multiline=self.settings.multiline)
		self.handler.setFormatter(formatter)  # type: ignore

		start = LoggingStartedRecord(pid=os.getpid())
		self.handler.emit(start)  # type: ignore

	def emit(self, record):
		self.handler.emit(record)

	def close(self):
		self.handler.close()

	def flush(self):
		self.handler.flush()

	@property
	def writes_to_stream(self) -> bool:
		return self.baseFilename is None

	@property
	def writes_to_file(self) -> bool:
		return self.baseFilename is not None

	@property
	def has_filter(self) -> bool:
		return True

	@property
	def has_formatter(self) -> bool:
		return True

	@property
	def needs_rulegraph(self) -> bool:
		return self.settings.rulegraph
