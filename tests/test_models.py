import logging

from snakemake_logger_plugin_json import models


def test_builtin_conversion(example_records: list[models.JsonLogRecord]):
	"""Test conversion to/from builtin LogRecord type."""

	for json_record in example_records:
		if isinstance(json_record, models.MetaLogRecord):
			continue

		model = type(json_record)

		builtin_record = json_record.to_builtin()
		assert isinstance(builtin_record, logging.LogRecord)

		json_record2 = model.from_builtin(builtin_record)
		assert type(json_record2) is model

		assert json_record2 == json_record
