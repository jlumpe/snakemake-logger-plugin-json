from argparse import ArgumentParser
import logging

from snakemake_logger_plugin_json import models
from snakemake_logger_plugin_json.json import parse_logfile
from snakemake_logger_plugin_json.stuff import RunStatus, JobInfo
from .app import LogfileApp


def add_fake_logs(logs: list[models.JsonLogRecord], insertat: int) -> None:
	t1 = logs[insertat - 1].created
	t2 = logs[insertat].created

	levels = [logging.NOTSET, logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]

	for i, levelno in enumerate(levels):
		levelname = logging.getLevelName(levelno)

		r = (i + 1) / (len(levels) + 1)
		time = r * t2 + (1 - r) * t1

		logs.insert(insertat + i, models.StandardLogRecord(
			levelno=levelno,
			levelname=levelname,
			message=f'test {levelname.lower()}',
			created=time,
		))


def load_run(logfile: str) -> RunStatus:

	with open(logfile) as fh:
		records = parse_logfile(fh)

		first = next(records)
		run = RunStatus(started=first.created_dt)
		run.process_record(first)

		for record in records:
			run.process_record(record)

	add_fake_logs(run.logs, 5)

	return run


def getapp() -> LogfileApp:
	parser = ArgumentParser()
	parser.add_argument('logfile')
	args = parser.parse_args()

	run = load_run(args.logfile)

	app = LogfileApp(run)
	return app


def main():
	app = getapp()
	app.run()
