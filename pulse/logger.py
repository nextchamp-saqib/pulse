import logging

import frappe
from frappe.utils.logger import set_log_level

from .constants import LOGGER_NAME

_logger = None


class FrappeErrorLogHandler(logging.Handler):
	"""Mirror Python logging records into Frappe Error Log doctype."""

	def emit(self, record: logging.LogRecord):
		try:
			# Use formatter if provided, else fallback to basic message
			msg = self.format(record) if self.formatter else record.getMessage()
			title = f"pulse:{record.levelname}".lower()
			# Store into Error Log doctype
			frappe.log_error(title=title, message=msg)
		except Exception:
			# Never raise from logging handler
			pass


def get_logger():
	global _logger
	if _logger is None:
		set_log_level("DEBUG")
		_logger = frappe.logger(LOGGER_NAME)
		# Attach the Error Log handler once
		if not any(isinstance(h, FrappeErrorLogHandler) for h in _logger.handlers):
			handler = FrappeErrorLogHandler()
			handler.setLevel(logging.DEBUG)
			handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s"))
			_logger.addHandler(handler)
	return _logger
