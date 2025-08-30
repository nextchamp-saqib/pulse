// Copyright (c) 2025, hello@frappe.io and contributors
// For license information, please see license.txt

frappe.ui.form.on("Warehouse Sync", {
	refresh(frm) {
		frm.add_custom_button("Sync Now", () => {
			frm.call('start_sync');
		});
	},
});
