from typing import Sequence
import logging
from pathlib import Path
import os

from snakemake_logger_plugin_json.logger import JsonLogHandlerSettings, JsonLogHandler
from snakemake_logger_plugin_json import models
from snakemake_logger_plugin_json.json import parse_logfile


def test_logging(
	example_records_standard: Sequence[models.JsonLogRecord],
	example_records_sm: Sequence[models.JsonLogRecord],
	tmp_path: Path,
):
	"""Test emitting example records through an actual logger and then parsing back again."""

	logfile = tmp_path / 'log'

	settings = JsonLogHandlerSettings(
		validate=True,
		file=str(logfile.absolute()),
	)
	handler = JsonLogHandler(None, settings)

	logger = logging.Logger('test', 0)
	logger.addHandler(handler)
	logger.propagate = False

	# Log
	records = (*example_records_standard, *example_records_sm)

	for record in records:
		logger.handle(record.to_builtin())

	handler.close()

	# Parse
	with open(logfile) as fh:
		parsed = list(parse_logfile(fh))

	# Check started/finished added
	assert len(parsed) == len(records) + 2
	assert isinstance(parsed[0], models.LoggingStartedRecord)
	assert parsed[0].pid == os.getpid()
	assert isinstance(parsed[-1], models.LoggingFinishedRecord)

	# Check emitted records
	for i, record in enumerate(records):
		assert parsed[i + 1] == record
