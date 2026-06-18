// End-to-end test for the Pulse telemetry pipeline.
//
// Lives in the pulse app because pulse is the downstream consumer: its CI has
// frappe (the client) installed, so it can validate the whole contract. frappe's
// CI cannot — it must not depend on pulse.
//
// Proves the chain on a single self-hosting site (pulse.localhost):
//
//   browser frappe.telemetry.capture()  [frappe pulse_client.js]
//     -> POST bulk_capture               [frappe client.py]
//     -> client Redis queue
//     -> send_queued_events()            -> POST bulk_ingest (X-Pulse-API-Key)
//     -> Pulse Redis stream
//     -> consume_pulse_events()          -> Pulse Event row  (asserted)
//
// Run against a site that has BOTH frappe and pulse installed:
//   bench --site pulse.localhost run-ui-tests pulse --headless \
//     --spec cypress/integration/pulse_telemetry.js
//
// Site used for the server-side drains is read from CYPRESS_pulse_site
// (default "pulse.localhost"); the api key from CYPRESS_pulse_api_key.

const SITE = Cypress.env("pulse_site") || "pulse.localhost";
const API_KEY = Cypress.env("pulse_api_key") || "asdf1234";
const EVENT_NAME = "cypress_pulse_e2e";
// Provisioned by pulse.e2e.setup — see pulse/e2e.py (E2E_USER / E2E_PASSWORD).
const E2E_USER = "pulse-e2e@example.com";
const E2E_PASSWORD = "Pulse-E2E-passw0rd!";

// bench lives at the bench root, two levels up from the pulse app (apps/pulse).
// Return cy.exec directly: wrapping it in a .then() that both calls a cy command
// and returns a value triggers Cypress's "mixing async and sync code" error.
function bench(fn) {
	return cy.exec(`cd ../.. && bench --site ${SITE} execute ${fn}`, { failOnNonZeroExit: true });
}

context("Pulse telemetry pipeline", () => {
	before(() => {
		// Force-enable the client on this dev site and sync the ingest api key,
		// then drop any rows left by a previous run. Must happen before the desk
		// loads so boot picks up enable_telemetry.
		bench(`pulse.e2e.setup --kwargs "{'api_key':'${API_KEY}'}"`);
		bench(`pulse.e2e.cleanup --kwargs "{'event_name':'${EVENT_NAME}'}"`);
	});

	it("captures a browser event and persists it as a Pulse Event", () => {
		cy.login(E2E_USER, E2E_PASSWORD);
		cy.visit("/app/home");

		// Telemetry must be live in the browser, otherwise the capture is dropped.
		cy.window().its("frappe.boot.enable_telemetry").should("eq", true);

		// Fire the event and force an immediate flush (skip the 10s timer / unload).
		cy.window().then((win) => {
			win.frappe.telemetry.capture(EVENT_NAME, "frappe", { source: "cypress" });

			const provider = win.frappe.telemetry.providers.find((p) => p.client);
			expect(provider, "pulse provider with client").to.exist;
			return provider.client.flush(); // resolves once the POST to bulk_capture completes
		});

		// Run both server hops by hand instead of waiting for the schedulers.
		bench("pulse.e2e.drain");

		// The event should now exist as a persisted Pulse Event row.
		cy.call("frappe.client.get_count", {
			doctype: "Pulse Event",
			filters: { event_name: EVENT_NAME },
		})
			.its("body.message")
			.should("be.gte", 1);
	});

	after(() => {
		bench(`pulse.e2e.cleanup --kwargs "{'event_name':'${EVENT_NAME}'}"`);
	});
});
