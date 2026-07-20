import frappe


def execute():
	"""Drop the orphaned ``received_at`` column from ``Pulse Event``.

	The receive time is the row's ``creation``. ``received_at`` was an earlier take on
	the same fact; the field was removed from the doctype without dropping the column,
	so it lingers in the table — populated until early 2026 and NULL for everything
	since, which makes it worse than absent for anyone who queries it. The events it
	described are unaffected, and their receive time is still on every row.

	Guarded, so it is a no-op wherever the column was never created.
	"""
	if not frappe.db.table_exists("Pulse Event"):
		return

	# Asked of the database rather than `db.has_column`, which answers from a cached
	# column list — stale exactly when a schema change is what we're deciding on.
	exists = frappe.db.sql(
		"""
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = %s AND column_name = 'received_at'
		""",
		("tabPulse Event",),
	)
	if exists:
		frappe.db.sql_ddl("ALTER TABLE `tabPulse Event` DROP COLUMN `received_at`")
