"""Read and write log records from/to JSON.
"""

import json
from typing import Any, Iterable, Iterator, Mapping, TypeAlias, get_args
from dataclasses import dataclass

from .models import JsonLogRecord, StandardLogRecord, adapter_cache, META_RECORD_MODELS, SNAKEMAKE_RECORD_MODELS


#
JsonData: TypeAlias = str | bytes | bytearray

JSON_DATA_TYPES: tuple[type, ...] = get_args(JsonData)


@dataclass
class JsonParseError(ValueError):

	msg: str
	data: Any = None
	start_line: int | None = None
	end_line: int | None = None

	def __post_init__(self):
		super().__init__(self.msg)


# ------------------------------------------------------------------------------------------------ #
#                                         Parse log records                                        #
# ------------------------------------------------------------------------------------------------ #

def _get_record_model(obj: Mapping[str, Any]) -> type[JsonLogRecord]:
	"""Determine the specific log record model class from parsed JSON data."""

	# Get base type
	if 'type' not in obj:
		raise JsonParseError('Record JSON missing "type" property', data=obj)

	typ = obj['type']

	# Standard log record
	if typ == 'standard':
		return StandardLogRecord

	# Other type
	if typ not in ('meta', 'snakemake'):
		raise JsonParseError(f'Invalid value for "type" property: {typ!r}', data=obj)

	if 'event' not in obj:
		raise JsonParseError(f'Record JSON of type {typ!r} missing "event" property', data=obj)

	event = obj['event']

	if typ == 'snakemake':
		if event not in SNAKEMAKE_RECORD_MODELS:
			raise JsonParseError(f'Invalid "event" property for snakemake record: {event!r}', data=obj)

		return SNAKEMAKE_RECORD_MODELS[event]

	else:
		if event not in META_RECORD_MODELS:
			raise JsonParseError(f'Invalid "event" property for meta record: {event!r}', data=obj)

		return META_RECORD_MODELS[event]


def logrecord_from_json(data: JsonData | Mapping[str, Any]) -> JsonLogRecord:
	"""Parse a log record from JSON data.

	Parameters
	----------
	data
		Either a JSON-encoded string/bytes or a parsed JSON object.
	"""

	if isinstance(data, JSON_DATA_TYPES):
		obj = json.loads(data)
		if not isinstance(obj, dict):
			raise ValueError('Parsed JSON value is not an object')
	elif isinstance(data, Mapping):
		obj = dict(data)
	else:
		raise TypeError('Expected JSON-encoded string/bytes or a mapping object')

	model = _get_record_model(obj)
	obj.pop('type', None)
	obj.pop('event', None)
	obj.pop('levelname', None)
	return adapter_cache.validate_python(model, obj)


# ------------------------------------------------------------------------------------------------ #
#                                          Parse log files                                         #
# ------------------------------------------------------------------------------------------------ #

_ObjParseResult = tuple[int, int, dict[str, Any]]


class JsonObjectParser:
	"""Lazily parses a file containing multiple JSON objects.

	Expects either of two formats (determined automatically, and may be mixed):

	* Single-line (JSONL): each line contains a complete JSON object.
	* Multi-line: Each JSON object spans multiple lines. The opening and closing braces must
	  appear on their own line with no indentation. Braces of nested objects must be indented or
	  appear with other non-whitespace characters. This is what you get with :func:`json.dump`
	  with a nonzero value for ``indent``.
	"""

	current_line: int

	def __init__(self):
		self.current_line = 0
		self._current_obj: list[str] = []
		self._current_started = 0

	def process_line(self, line: str) -> _ObjParseResult | None:
		"""Process a single line. If it concludes an object, return it."""
		self.current_line += 1

		# Ignore trailing whitespace but not leading
		line = line.rstrip()

		# Skip blank lines
		if line.isspace():
			return None

		# Already in the middle of a multi-line object?
		if self._current_obj:
			self._current_obj.append(line)

			# Object completed?
			if line == '}':
				data = ''.join(self._current_obj)
				try:
					value = json.loads(data)
				except json.JSONDecodeError as exc:
					raise JsonParseError(
						str(exc),
						start_line=self._current_started,
						end_line=self.current_line,
					) from exc

				rval = (self._current_started, self.current_line, value)
				self._current_obj = []
				self._current_started = 0
				return rval

			return None

		# Starting a new multi-line object?
		if line == '{':
			self._current_obj = [line]
			self._current_started = self.current_line
			return

		# Otherwise expect complete object on single line
		if line.startswith('{'):
			try:
				value = json.loads(line)
			except json.JSONDecodeError:
				pass
			else:
				if isinstance(value, dict):
					return (self.current_line, self.current_line, value)

		raise JsonParseError(
			'Expected single opening brace or complete JSON object',
			start_line=self.current_line,
			end_line=self.current_line,
		)

	def process_lines(self, lines: Iterable[str]) -> Iterable[_ObjParseResult]:
		"""Process multiple lines and yield all complete objects parsed."""
		for line in lines:
			result = self.process_line(line)
			if result is not None:
				yield result

	def complete(self) -> None:
		"""Signal that there are no more lines available.

		This will raise an exception if the final JSON object has not been concluded.
		"""
		if self._current_obj:
			raise JsonParseError(
				f'JSON object starting on line {self._current_started} not closed',
				start_line=self._current_started,
				end_line=self.current_line,
			)


def parse_logfile(lines: Iterable[str]) -> Iterator[JsonLogRecord]:
	parser = JsonObjectParser()

	for l1, l2, obj in parser.process_lines(lines):
		yield logrecord_from_json(obj)

	parser.complete()
