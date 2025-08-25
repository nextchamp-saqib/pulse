// Copyright (c) 2025, hello@frappe.io and contributors
// For license information, please see license.txt

frappe.ui.form.on("Data Sync", {
	refresh(frm) {
		frm.add_custom_button(__('Sync'), () => {
			frm.call('sync');
		});
	},
});
