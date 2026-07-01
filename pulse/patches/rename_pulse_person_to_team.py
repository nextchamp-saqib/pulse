import frappe


def execute():
	"""Rename ``Pulse Person`` -> ``Pulse Team`` and the subject field ``user`` -> ``team``.

	The identity subject is the Frappe Cloud team, so both the doctype and its subject
	field were renamed (``Pulse Alias`` keeps its name but its subject field is renamed
	too). Runs before model sync so the table/columns are renamed in place instead of
	sync creating fresh ones and orphaning the old. Every step is guarded, so it's a
	no-op on fresh installs (tables not created yet) and on re-runs.
	"""
	_rename_column("Pulse Alias", "user", "team")

	if frappe.db.table_exists("Pulse Person") and not frappe.db.table_exists("Pulse Team"):
		# Rename the column while the table is still `tabPulse Person`, then the doctype.
		_rename_column("Pulse Person", "user", "team")
		frappe.rename_doc("DocType", "Pulse Person", "Pulse Team", force=True)


def _rename_column(doctype: str, old: str, new: str):
	if (
		frappe.db.table_exists(doctype)
		and frappe.db.has_column(doctype, old)
		and not frappe.db.has_column(doctype, new)
	):
		frappe.db.rename_column(doctype, old, new)
