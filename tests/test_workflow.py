"""Test actually running a workflow with the logger."""

from pathlib import Path
from subprocess import run
from warnings import warn

import pytest

from snakemake_logger_plugin_json.json import logrecord_from_json
from snakemake_logger_plugin_json.models import ALL_MODELS


TEST_DIR = Path(__file__).parent


def test_run_workflow(tmp_path: Path):
	"""Test running the example workflow with the logger and parsing the output."""

	workdir = tmp_path / 'workdir'
	workdir.mkdir()
	logfile = tmp_path / 'log'

	# Run snakemake
	cmd = [
		'snakemake',
		'-s', str(TEST_DIR / 'workflow/Snakefile'),
		'-d', str(workdir.absolute()),
		'-c', '4',
		'--logger', 'json',
		'--logger-json-file', str(logfile.absolute()),
		'--logger-json-validate',
		'--logger-json-rulegraph',
	]
	run(cmd, check=True)

	# Parse
	records = []

	with open(logfile) as fh:
		for line in fh:
			records.append(logrecord_from_json(line))

	# Ideally we'd like to generate and test all types of record. Add a warning about record types
	# which were not generated.
	seen_types = {type(record) for record in records}
	missing_types = set(ALL_MODELS) - seen_types
	if missing_types:
		names = ', '.join(sorted(typ.__name__ for typ in missing_types))
		warn(f'The following record types were not generated: {names}')
