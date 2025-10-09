import logging
from dataclasses import dataclass, field
import json
from datetime import datetime
import os

from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

from .json import JsonSerializer
from .models import JsonLogRecord, FormattingErrorRecord


def make_logfile_path(workdir: os.PathLike | None = None, timestamp: datetime | None = None) -> str:

	if timestamp is None:
		timestamp = datetime.now()

	filename = datetime.now().isoformat().replace(':', '') + '.log'

	path = os.path.join('.snakemake/log/json', filename)
	if workdir is not None:
		path = os.path.join(workdir, path)

	return path


@dataclass
class JsonLogHandlerSettings(LogHandlerSettingsBase):
	file: str = field(default=None, metadata={
		'help': 'File to write to (or - to use stderr).',
	})
	oneline: bool = field(default=False, metadata={
		'help': 'Output in jsonl format (one line per record).',
	})
	rulegraph: bool = field(default=False, metadata={
		'help': 'Output rule graph.',
	})


@dataclass
class JsonFormatter:
	"""Log formatter."""

	oneline: bool = False
	serializer: JsonSerializer = field(init=False)

	def __post_init__(self) -> None:
		self.serializer = JsonSerializer()
		self.encoder = json.JSONEncoder(
			default=self.serializer.serialize,
			indent=None if self.oneline else 4,
			sort_keys=False,
		)

	def format(self, record: logging.LogRecord) -> str:
		try:
			json_record = JsonLogRecord.from_builtin(record)
			adapter = TypeAdapter(type(json_record))
			adapter.validate_python(json_record.__dict__)
		except Exception as exc:
			import pdbr; pdbr.post_mortem()
			sys.exit()
			json_record = FormattingErrorRecord.create(record, exc)
		return self.encoder.encode(json_record)


class JsonLogHandler(LogHandlerBase):

	settings: JsonLogHandlerSettings
	baseFilename: str | None
	handler: logging.Handler

	def __init__(self, *args):
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

		formatter = JsonFormatter(oneline=self.settings.oneline)
		self.handler.setFormatter(formatter)

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
