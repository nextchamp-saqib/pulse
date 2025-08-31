import frappe
from frappe.model.document import Document


class PulseLog(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		level: DF.Literal["INFO", "ERROR", "DEBUG", "WARNING"]
		message: DF.LongText | None
	# end: auto-generated types

	def db_insert(self, *args, **kwargs):
		raise NotImplementedError

	def load_from_db(self):
		entries = read_pulse_log()
		entry = next((e for e in entries if e["id"] == self.name), None)

		if entry:
			doc = {
				"name": entry["id"],
				"level": entry.get("level"),
				"message": entry["message"],
				"creation": entry.get("timestamp"),
				"modified": entry.get("timestamp"),
			}
		else:
			doc = {"name": self.name, "message": ""}

		super(Document, self).__init__(doc)

	def db_update(self):
		raise NotImplementedError

	def delete(self):
		raise NotImplementedError

	@staticmethod
	def get_list(filters=None, page_length=20, **kwargs):
		start = int(kwargs.get("start", 0))
		page_length = int(page_length or 20)

		level_filter = None
		if isinstance(filters, dict):
			level_filter = filters.get("level")
		if isinstance(filters, list) and len(filters) and len(filters[0]) == 4:
			for f in filters:
				if f[1] == "level" and f[2] == "=":
					level_filter = f[3]

		entries = read_pulse_log()
		entries.reverse()

		if level_filter:
			entries = [e for e in entries if e.get("level") == level_filter]

		return [
			{
				"name": e["id"],
				"level": e.get("level"),
				"message": e["message"],
				"creation": e.get("timestamp"),
			}
			for e in entries[start : start + page_length]
		]

	@staticmethod
	def get_count(filters=None, **kwargs):
		entries = read_pulse_log()

		level_filter = None
		if isinstance(filters, dict):
			level_filter = filters.get("level")
		if isinstance(filters, list) and len(filters) and len(filters[0]) == 4:
			for f in filters:
				if f[1] == "level" and f[2] == "=":
					level_filter = f[3]

		if level_filter:
			entries = [e for e in entries if e.get("level") == level_filter]

		return len(entries)

	@staticmethod
	def get_stats(**kwargs):
		pass


def parse_log_line(line):
	line = line.strip()
	if not line:
		return None

	parts = line.split(None, 3)
	if len(parts) < 3:
		return None

	timestamp = f"{parts[0]} {parts[1]}"

	level_part = parts[2]
	if level_part.startswith("[") and level_part.endswith("]:"):
		level = level_part[1:-2]
		message = parts[3] if len(parts) > 3 else ""
	elif level_part in ["INFO", "ERROR", "DEBUG", "WARNING"]:
		level = level_part
		message = " ".join(parts[3:]) if len(parts) > 3 else ""
	else:
		level = None
		message = " ".join(parts[2:])

	return {"timestamp": timestamp, "level": level, "message": message}


def read_pulse_log():
	log_path = frappe.get_site_path("logs", "pulse.log")

	try:
		with open(log_path, "rb") as file:
			file.seek(0, 2)
			file_size = file.tell()

			chunk_size = 1024 * 1024 # 1 MB
			file.seek(max(0, file_size - chunk_size))

			data = file.read().decode("utf-8", errors="replace")
			lines = data.split("\n")

			if file_size > chunk_size and lines:
				lines = lines[1:]
	except (FileNotFoundError, Exception):
		return []

	entries = []
	current_entry = None
	entry_index = 0

	for line in lines:
		parsed = parse_log_line(line)

		if parsed:
			if current_entry:
				entries.append(current_entry)

			current_entry = {
				"id": str(entry_index),
				"timestamp": parsed["timestamp"],
				"level": parsed["level"],
				"message": parsed["message"].lstrip("pulse "),
			}
			entry_index += 1
		else:
			if current_entry:
				current_entry["message"] += "\n" + line.rstrip("\n")

	if current_entry:
		entries.append(current_entry)

	return entries
