import logging

import pytest

import snakemake_logger_plugin_json.json as json_module
from snakemake_logger_plugin_json import models


RANDOM_TIMESTAMP = 1759974850.185749


@pytest.fixture(scope='session')
def example_records() -> list[models.JsonLogRecord]:
	"""List of example JsonLogRecord instances, one of each subclass."""

	i = 0
	levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]

	def make_record(cls, **kw):
		nonlocal i
		i += 1

		kw.setdefault('message', f'Test {cls}')
		kw.setdefault('levelno', levels[i % len(levels)])
		kw.setdefault('created', RANDOM_TIMESTAMP + i * 5.13917)

		from dataclasses import fields
		fieldnames = {field.name for field in fields(cls)}
		for name in kw:
			if name not in fieldnames:
				raise ValueError(f'Unknown field: {name!r}')

		return models.adapter_cache.validate_python(cls, kw)

	return [
		make_record(models.StandardLogRecord),
		make_record(models.LoggingStartedRecord, pid=1234, proc_started=RANDOM_TIMESTAMP),
		make_record(models.FormattingErrorRecord, record_partial={'foo': 'bar'}),
		make_record(
			models.ErrorRecord,
			exception='some error',
			location='somewhere',
			rule='rule_name',
			file='script.py',
			# line=123,
		),
		make_record(
			models.WorkflowStartedRecord,
			workflow_id='f0915278-1f9d-4cc8-a2b3-f23c3649c7e4',
			snakefile='/path/to/snakefile',
		),
		make_record(
			models.JobInfoRecord,
			jobid=123,
			rule_name='rule_name',
			threads=4,
			input=['in/file1', 'in/file2'],
			output=['out/file3'],
			wildcards={'foo': '1'},
		),
		make_record(
			models.JobStartedRecord,
			jobs=[1, 2, 3],
		),
		make_record(
			models.JobFinishedRecord,
			job_id=123,
		),
		make_record(
			models.ShellCmdRecord,
			jobid=123,
			shellcmd='echo hello',
			rule_name='some_rule',
		),
		make_record(
			models.JobErrorRecord,
			jobid=123,
		),
		make_record(
			models.GroupInfoRecord,
			group_id=123,
			jobs=[56, 78],
		),
		make_record(
			models.GroupErrorRecord,
			groupid=123,
			aux_logs=['one', 'two'],
			job_error_info={},
		),
		make_record(
			models.ResourcesInfoRecord,
			# nodes=?,
			cores=10,
			# provided_resources=?,
		),
		make_record(
			models.DebugDagRecord,
			status='status',
			job=123,
			file='file.py',
			exception='some error',
		),
		make_record(
			models.ProgressRecord,
			done=34,
			total=56,
		),
		make_record(
			models.RulegraphRecord,
			rulegraph={},
		),
		make_record(
			models.RunInfoRecord,
			stats={},
		),
	]


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
