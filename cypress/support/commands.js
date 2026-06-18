// Minimal subset of frappe's cypress commands, vendored so pulse's e2e suite is
// self-contained (run from the pulse app root by `run-ui-tests pulse`).

Cypress.Commands.add("login", (email, password) => {
	if (!email) {
		email = Cypress.config("testUser") || "Administrator";
	}
	if (!password) {
		password = Cypress.env("adminPassword");
	}
	return cy.session([email, password] || "", () => {
		return cy.request({
			url: "/api/method/login",
			method: "POST",
			body: { usr: email, pwd: password },
		});
	});
});

Cypress.Commands.add("call", (method, args) => {
	return cy
		.window()
		.its("frappe.csrf_token")
		.then((csrf_token) => {
			return cy
				.request({
					url: `/api/method/${method}`,
					method: "POST",
					body: args,
					headers: {
						Accept: "application/json",
						"Content-Type": "application/json",
						"X-Frappe-CSRF-Token": csrf_token,
					},
				})
				.then((res) => {
					expect(res.status).eq(200);
					return res;
				});
		});
});
