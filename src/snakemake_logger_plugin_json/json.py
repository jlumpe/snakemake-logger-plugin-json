"""Read and write log records from/to JSON.
"""

from typing import Any
from functools import singledispatchmethod
from uuid import UUID
from os import PathLike, fspath
from dataclasses import dataclass, fields

from .models import JsonLogRecord, SnakemakeLogRecord


@dataclass
class JsonSerializer:

	@singledispatchmethod
	def serialize(self, value: Any) -> Any:
		return value

	@serialize.register
	def _serialize_uuid(self, value: UUID):
		return str(value)

	@serialize.register
	def _serialize_pathlike(self, value: PathLike):
		return fspath(value)

	@serialize.register
	def _serialize_logrecord(self, value: JsonLogRecord):
		d = dict(type=value.type)
		if isinstance(value, SnakemakeLogRecord):
			d['event'] = value.event.value

		for field in fields(value):
			d[field.name] = getattr(value, field.name)

		return d
