"""Snakemake logger plugin that emits JSON-formatted records."""

__author__ = 'Jared Lumpe'
__email__ = 'jared@jaredlumpe.com'


# Import as standard names for plugin registration to work
from .logger import JsonLogHandler as LogHandler, JsonLogHandlerSettings as LogHandlerSettings
