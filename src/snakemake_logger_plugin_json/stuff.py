from typing import Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from functools import singledispatchmethod
from uuid import UUID

from . import models


@dataclass
class JobInfo:

	id: int

	rule_name: str
	threads: int
	input: list[str] | None = None
	output: list[str] | None = None
	log: list[str] | None = None
	benchmark: list[str] | None = None
	# rule_msg: str | None = None
	wildcards: dict[str, Any] | None = None
	reason: str | None = None
	shellcmd: str | None = None
	priority: int | None = None
	# resources: dict[str, Any] | list[Any] | None = None

	started: datetime | None = None
	finished: datetime | None = None
	logs: list[models.SnakemakeLogRecord] = field(default_factory=list)

	@staticmethod
	def from_record(record: models.JobInfoRecord) -> 'JobInfo':
		return JobInfo(
			id=record.jobid,
			rule_name=record.rule_name,
			threads=record.threads,
			input=record.input,
			output=record.output,
			log=record.log,
			benchmark=record.benchmark,
			wildcards=record.wildcards,
			priority=record.priority,
			started=record.created_dt,
			logs=[record],
		)


@dataclass(kw_only=True, repr=False)
class RunStatus:

	started: datetime
	finished: datetime | None = None
	workdir: Path | None = None
	workflow_id: UUID | None = None
	snakefile: Path | None = None
	rulegraph: Any = None

	logs: list[models.JsonLogRecord] = field(default_factory=list)
	jobs: dict[int, JobInfo] = field(default_factory=dict)

	def __post_init__(self):
		self._job_logs: dict[int, list[models.SnakemakeLogRecord]] = dict()

	# ---------------------------------------- Process records --------------------------------------- #

	def _process_record_base(self, record: models.JsonLogRecord):
		self.logs.append(record)

	process_record = singledispatchmethod(_process_record_base)

	@process_record.register
	def _process_snakemake_record(self, record: models.SnakemakeLogRecord, add_to_job: bool = True):
		self._process_record_base(record)

		if add_to_job:
			for jobid in record.associated_jobs():
				if jobid in self.jobs:
					self.jobs[jobid].logs.append(record)
				else:
					self._job_logs.setdefault(jobid, []).append(record)

	@process_record.register
	def _process_job_info(self, record: models.JobInfoRecord):
		if record.jobid in self.jobs:
			raise ValueError(f'Duplicate JOB_INFO event for job {record.jobid}')

		job = JobInfo.from_record(record)
		self.jobs[record.jobid] = job

		if job.id in self._job_logs:
			job.logs.extend(self._job_logs.pop(job.id))

		self._process_snakemake_record(record, add_to_job=False)

	@process_record.register
	def _process_job_finished(self, record: models.JobFinishedRecord):
		if record.job_id not in self.jobs:
			raise ValueError(f'JOB_FINISHED event before JOB_INFO for job {record.job_id}')

		job = self.jobs[record.job_id]
		if job.finished:
			raise ValueError(f'Duplicate JOB_FINISHED event for job {job.id}')
		job.finished = record.created_dt

		self._process_snakemake_record(record)

	@process_record.register
	def _process_workflow_started(self, record: models.WorkflowStartedRecord):
		if self.workflow_id is not None:
			raise ValueError('Duplicate WORKFLOW_STARTED event')
		self.workflow_id = record.workflow_id
		if record.snakefile is not None:
			self.snakefile = Path(record.snakefile)
		self._process_snakemake_record(record)

	@process_record.register
	def _process_rulegraph(self, record: models.RulegraphRecord):
		if self.rulegraph is not None:
			raise ValueError('Duplicate RULEGRAPH event')
		self.rulegraph = record.rulegraph
		self._process_snakemake_record(record)

	# ---------------------------------------- Saving/loading ---------------------------------------- #

	def dump_json(self) -> dict[str, Any]:
		raise NotImplementedError()

	@staticmethod
	def load_json(data: dict[str, Any]) -> 'RunStatus':
		raise NotImplementedError()
