from datetime import timedelta
import os
from typing import Any
import logging

from textual.app import App, ComposeResult
from textual.containers import HorizontalGroup, VerticalGroup, VerticalScroll
from textual import widgets
from textual.widget import Widget
from textual.widgets import (
	Footer, Header, DataTable, TabbedContent, TabPane, Label, Static
)
from textual.style import Style
from textual.content import Content
from textual.reactive import reactive
from rich.text import Text

from snakemake_logger_plugin_json import models
from snakemake_logger_plugin_json.stuff import RunStatus, JobInfo


LEVELS = [
	logging.DEBUG,
	logging.INFO,
	logging.WARNING,
	logging.ERROR,
	logging.CRITICAL,
]
LEVEL_NAMES = list(map(logging.getLevelName, LEVELS))


def format_td(td: timedelta) -> str:
	secs = td.total_seconds()
	if secs < 0:
		neg = True
		secs = -secs
	else:
		neg = False

	secs = int(secs)
	mins, secs = divmod(secs, 60)
	hours, mins = divmod(mins, 60)

	s = f'{hours:d}:{mins:02d}:{secs:02d}'
	if neg:
		s = '-' + s
	return s


# ---------------------------------------------------------------------------- #
#                                    Widgets                                   #
# ---------------------------------------------------------------------------- #


class StaticTable(DataTable):

	def __init__(self, items: list[tuple[Any, Any]] | None = None):
		super().__init__(
			show_header=False,
			show_cursor=False,
		)
		super().add_column('Value', key='value')

		if items is not None:
			for label, value in items:
				self.add_item(label, value)

	def add_item(self, label, value, key=None, *, height: int | None = 1):
		super().add_row(value, label=label, key=key, height=height)

	def add_column(self, *args, **kw):
		raise RuntimeError('Not allowed')

	def add_row(self, *args, **kw):
		raise RuntimeError('Not allowed')


# ---------------------------------------------------------------------------- #
#                                  Log screen                                  #
# ---------------------------------------------------------------------------- #

class LogDetails(VerticalScroll):

	record: reactive[models.JsonLogRecord | None] = reactive(None, recompose=True)
	rundata: RunStatus

	def __init__(self, rundata: RunStatus):
		super().__init__()
		self.rundata = rundata

	def compose(self) -> ComposeResult:
		self._update_class()

		if self.record is None:
			self.border_title = None
			yield widgets.Static('No record selected')
			return

		self.border_title = self.record.levelname or '?'

		# Basic attrs
		yield self._make_basic_table(self.record)

		# Message
		yield Label(self.record.message or '', id='message')

		# Additional
		addl = self._make_additional_data(self.record)
		if addl:
			yield addl

	def _update_class(self) -> None:
		classes = []
		if self.record is None:
			classes.append('record-none')
		elif self.record.levelname in LEVEL_NAMES:
			classes.append('record-' + self.record.levelname.lower())
		else:
			classes.append('record-other')

		if self.record is not None:
			classes.append('record-' + self.record.type)

		self.set_classes(classes)

	def _make_basic_table(self, record: models.JsonLogRecord) -> StaticTable:
		table = StaticTable()

		time = record.created_dt - self.rundata.started
		table.add_item('Time', format_td(time))

		event = getattr(record, 'event', None)
		if event is not None:
			table.add_item('Event', str(event))

		if isinstance(record, models.SnakemakeLogRecord):
			jobs = record.associated_jobs()
			if jobs:
				table.add_item('Jobs', ' '.join(map(str, sorted(jobs))))

		return table

	def _make_additional_data(self, record: models.JsonLogRecord) -> Widget | None:
		if isinstance(record, models.StandardLogRecord):
			return None

		attrs = dict()

		import dataclasses

		record_fields = set(field.name for field in dataclasses.fields(type(record)))
		base_fields = set(field.name for field in dataclasses.fields(models.JsonLogRecord))
		fields = record_fields - base_fields
		fields -= {'jobid', 'job_id', 'job_ids', 'jobs'}

		if not fields:
			return None

		table = StaticTable()
		from rich.pretty import Pretty

		for name in fields:
			value = getattr(record, name)
			if isinstance(value, (str, int, bool, os.PathLike)):
				value = str(value)
			elif value is None:
				value = '[dim]None[/]'
			else:
				value = Pretty(value)
			table.add_item(name, value, height=None)

		return table


class LogScreen(HorizontalGroup):

	_COLUMNS: list[tuple[str, str, int]] = [
		('time', 'Time', 8),
		('info', 'Info', 4),
		('event', 'Event', 15),
		('message', 'Message', 10),
	]

	rundata: RunStatus

	def __init__(self, rundata: RunStatus):
		super().__init__()
		self.rundata = rundata

	def compose(self) -> ComposeResult:
		yield DataTable(
			cursor_type='row',
		)
		details = LogDetails(self.rundata)
		details.record = self.rundata.logs[0]
		yield details

	def on_mount(self) -> None:
		table: DataTable = self.query_one(DataTable)
		# table: ResizingDataTable = self.query_one(ResizingDataTable)
		table.show_horizontal_scrollbar = False

		for key, label, width in self._COLUMNS:
			# table.add_column(label, key=key, width=width)
			table.add_column(label, key=key)

		self._populate_table(table)

	def _populate_table(self, table: DataTable):
		for i, record in enumerate(self.rundata.logs):
			self._add_row(table, record, i)

	def _add_row(self, table: DataTable, record: models.JsonLogRecord, i: int):
		event = getattr(record, 'event', None)
		style = Style()

		time = record.created_dt - self.rundata.started

		if record.levelname == 'INFO':
			info = Content('i')
		elif record.levelname in ('ERROR', 'CRITICAL'):
			style = Style.parse('$text-error bold')
			info = Content.from_markup('[$error on $error-muted]' + record.levelname[0])
		elif record.levelname == 'WARNING':
			style = Style.parse('$text-warning')
			info = Content.from_markup('[bold white on $warning-muted]W')
		elif record.levelname == 'DEBUG':
			style = Style.parse('dim')
			info = Content.styled('d', style)
		else:
			info = Content('?')

		time = Text(format_td(time), style.rich_style)
		message = Text(record.message or '', style=style.rich_style, overflow='ellipsis')

		table.add_row(
			time,
			info,
			event,
			message,
			key=str(i),
		)

	def on_data_table_row_highlighted(self, event: DataTable.RowSelected):
		details: LogDetails = self.query_one(LogDetails)
		key = event.row_key.value

		if key is None:
			details.record = None
			return

		try:
			row = int(key)
		except ValueError:
			pass
		else:
			if 0 <= row < len(self.rundata.logs):
				details.record = self.rundata.logs[row]
				return

		details.record = None


# ---------------------------------------------------------------------------- #
#                                  Job screen                                  #
# ---------------------------------------------------------------------------- #

class JobsScreen(HorizontalGroup):

	rundata: RunStatus

	def __init__(self, rundata: RunStatus):
		super().__init__()
		self.rundata = rundata

	def compose(self) -> ComposeResult:
		yield DataTable(
			cursor_type='row',
		)

	def on_mount(self) -> None:
		table: DataTable = self.query_one(DataTable)
		table.add_columns(
			'Rule',
			'Started',
			'Duration',
		)

		self._populate_table(table)

	def _populate_table(self, table: DataTable):
		for job in self.rundata.jobs.values():
			self._add_row(table, job)

	def _add_row(self, table: DataTable, job: JobInfo):
		if job.started:
			started = format_td(job.started - self.rundata.logs[0].created_dt)
		else:
			started = ''

		if job.started and job.finished:
			duration = format_td(job.finished - job.started)
		else:
			duration = ''

		table.add_row(
			job.rule_name,
			started,
			duration,
			key=str(job.id),
			label=str(job.id),
		)


# ---------------------------------------------------------------------------- #
#                                      App                                     #
# ---------------------------------------------------------------------------- #

class LogfileApp(App):
	CSS_PATH = 'style.tcss'

	BINDINGS = [
		('l', 'show_tab("log")', 'Log'),
		('j', 'show_tab("jobs")', 'Jobs'),
	]

	rundata: RunStatus

	def __init__(self, rundata: RunStatus):
		super().__init__()
		self.rundata = rundata

	def compose(self) -> ComposeResult:
		"""Called to add widgets to the app."""
		yield Header()
		yield Footer()

		with TabbedContent():
			with TabPane('Log', id='log'):
				yield LogScreen(self.rundata)
			with TabPane('Jobs', id='jobs'):
				yield JobsScreen(self.rundata)
			with TabPane('Test', id='test'):
				yield widgets.Label('Test')

	def action_show_tab(self, tabid: str) -> None:
		self.query_one(TabbedContent).active = tabid
