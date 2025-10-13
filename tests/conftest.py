from typing import Sequence, TypeVar
import logging
from dataclasses import dataclass, fields

import pytest

from snakemake_logger_plugin_json import models


M = TypeVar('M', bound=models.JsonLogRecord)


RANDOM_TIMESTAMP = 1759974850.185749
LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]


def make_record(cls: type[M], **kw) -> M:
	kw.setdefault('message', f'Test {cls}')
	kw.setdefault('levelno', logging.INFO)
	kw.setdefault('created', RANDOM_TIMESTAMP)

	fieldnames = {field.name for field in fields(cls)}
	for name in kw:
		if name not in fieldnames:
			raise ValueError(f'Unknown field: {name!r}')

	return models.adapter_cache.validate_python(cls, kw)


@dataclass
class RecordFactory:
	"""
	Cycles through some different values for attribute defaults.
	"""

	i: int = 0

	def make_record(self, cls: type[M], **kw) -> M:
		kw.setdefault('levelno', LEVELS[self.i % len(LEVELS)])
		kw.setdefault('created', RANDOM_TIMESTAMP + self.i * 5.13917)
		self.i += 1
		return make_record(cls, **kw)


@pytest.fixture(scope='session')
def example_records_sm() -> Sequence[models.SnakemakeLogRecord]:
	"""Example SnakemakeLogRecord instances, one of each subclass."""

	factory = RecordFactory()

	return (
		factory.make_record(
			models.ErrorRecord,
			exception='some error',
			location='somewhere',
			rule='rule_name',
			file='script.py',
			# line=123,
		),
		factory.make_record(
			models.WorkflowStartedRecord,
			workflow_id='f0915278-1f9d-4cc8-a2b3-f23c3649c7e4',
			snakefile='/path/to/snakefile',
		),
		factory.make_record(
			models.JobInfoRecord,
			jobid=123,
			rule_name='rule_name',
			threads=4,
			input=['in/file1', 'in/file2'],
			output=['out/file3'],
			wildcards={'foo': '1'},
		),
		factory.make_record(
			models.JobStartedRecord,
			jobs=[1, 2, 3],
		),
		factory.make_record(
			models.JobFinishedRecord,
			job_id=123,
		),
		factory.make_record(
			models.ShellCmdRecord,
			jobid=123,
			shellcmd='echo hello',
			rule_name='some_rule',
		),
		factory.make_record(
			models.JobErrorRecord,
			jobid=123,
		),
		factory.make_record(
			models.GroupInfoRecord,
			group_id=123,
			jobs=[56, 78],
		),
		factory.make_record(
			models.GroupErrorRecord,
			groupid=123,
			aux_logs=['one', 'two'],
			job_error_info={},
		),
		factory.make_record(
			models.ResourcesInfoRecord,
			# nodes=?,
			cores=10,
			# provided_resources=?,
		),
		factory.make_record(
			models.DebugDagRecord,
			status='status',
			job=123,
			file='file.py',
			exception='some error',
		),
		factory.make_record(
			models.ProgressRecord,
			done=34,
			total=56,
		),
		factory.make_record(
			models.RulegraphRecord,
			rulegraph={},
		),
		factory.make_record(
			models.RunInfoRecord,
			stats={},
		),
	)


@pytest.fixture(scope='session')
def example_records_meta() -> Sequence[models.MetaLogRecord]:
	"""Example MetaLogRecord instances, one of each subclass."""

	factory = RecordFactory()

	return (
		factory.make_record(models.LoggingStartedRecord, pid=1234, proc_started=RANDOM_TIMESTAMP),
		factory.make_record(models.LoggingFinishedRecord),
		factory.make_record(models.FormattingErrorRecord, record_partial={'foo': 'bar'}),
	)


@pytest.fixture(scope='session')
def example_records_standard() -> Sequence[models.StandardLogRecord]:
	"""Example StandardLogRecord instances, one of each level."""

	factory = RecordFactory()

	return tuple(
		factory.make_record(models.StandardLogRecord, levelno=level)
		for level in LEVELS
	)


@pytest.fixture(scope='session')
def example_records(
	example_records_standard,
	example_records_sm,
	example_records_meta,
) -> Sequence[models.JsonLogRecord]:
	"""Example JsonLogRecord instances, one of each subclass."""
	return (*example_records_standard, *example_records_sm, *example_records_meta)
