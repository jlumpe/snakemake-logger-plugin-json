import json

import snakemake_logger_plugin_json.json as json_module
from snakemake_logger_plugin_json import models


def test_record_roundtrip(example_records):
	"""Test round-tripping records to JSON."""

	for record in example_records:
		dumped_bytes = models.adapter_cache.dump_json(record)
		# dumped_python = models.adapter_cache.dump_python(record, mode='json')
		dumped_python = json.loads(dumped_bytes)

		for data in [dumped_bytes, dumped_python]:
			parsed = json_module.logrecord_from_json(data)
			assert type(parsed) is type(record)
			assert parsed == record
