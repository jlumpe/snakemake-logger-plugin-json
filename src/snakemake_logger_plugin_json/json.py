"""Read and write log records from/to JSON.
"""

import json
from typing import Any, Iterable, Mapping, TypeAlias, get_args
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
		if not isinstance(obj, Mapping):
			raise ValueError('Parsed JSON value is not an object')
	elif isinstance(data, Mapping):
		obj = data
	else:
		raise TypeError('Expected JSON-encoded string/bytes or a mapping object')

	model = _get_record_model(obj)
	return adapter_cache.validate_python(model, obj)
