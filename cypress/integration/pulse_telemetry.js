// End-to-end + HTTP-contract tests for the Pulse telemetry pipeline.
//
// Lives in the pulse app because pulse is the downstream consumer: its CI has
// frappe (the client) installed, so it can validate the whole contract. frappe's
// CI cannot — it must not depend on pulse.
//
// The browser now posts directly to pulse (no framework relay). The pipeline spec
// proves the chain on a single self-hosting site (pulse.localhost), where browser
// -> pulse is same-origin:
//
//   browser frappe.telemetry.capture()   [pulse_client.js, loaded from /assets/pulse/]
//     -> POST pulse.api.bulk_ingest        (form-encoded, key in body, credentials: omit)
//     -> Pulse Redis stream
//     -> consume_pulse_events()            -> Pulse Event row  (asserted)
//
// CORS itself is owned by nginx (the site's `allow_cors`), not the app — the ingest
// is a preflight-free "simple" request, so there's no app CORS to test. The
// HTTP-contract spec just drives the endpoint with cy.request to assert auth.
//
// Run against a site that has BOTH frappe and pulse installed:
//   bench --site pulse.localhost run-ui-tests pulse --headless \
//     --spec cypress/integration/pulse_telemetry.js
//
// Site used for the bench hops is read from CYPRESS_pulse_site (default
// "pulse.localhost"); the api key from CYPRESS_pulse_api_key.

const SITE = Cypress.env("pulse_site") || "pulse.localhost";
const API_KEY = Cypress.env("pulse_api_key") || "asdf1234";
// Where the browser client posts (and loads itself) from — the local pulse, not
// the real CDN. Same origin as baseUrl, so the ingest POST needs no CORS.
const HOST = Cypress.env("pulse_host") || Cypress.config("baseUrl") || "http://pulse.localhost:8000";
const EVENT_NAME = "cypress_pulse_e2e";
// Provisioned by pulse.e2e.setup — see pulse/e2e.py (E2E_USER / E2E_PASSWORD).
const E2E_USER = "pulse-e2e@example.com";
const E2E_PASSWORD = "Pulse-E2E-passw0rd!";

const INGEST_URL = `${HOST}/api/method/pulse.api.bulk_ingest`;

// bench lives at the bench root, two levels up from the pulse app (apps/pulse).
// Return cy.exec directly: wrapping it in a .then() that both calls a cy command
// and returns a value triggers Cypress's "mixing async and sync code" error.
function bench(fn) {
	return cy.exec(`cd ../.. && bench --site ${SITE} execute ${fn}`, { failOnNonZeroExit: true });
}

// Force-enable the client, point it at the local pulse, and sync the ingest key.
// Must run before the desk loads so boot picks it up.
before(() => {
	bench(`pulse.e2e.setup --kwargs "{'api_key':'${API_KEY}','host':'${HOST}'}"`);
});

context("Pulse telemetry pipeline", () => {
	before(() => {
		bench(`pulse.e2e.cleanup --kwargs "{'event_name':'${EVENT_NAME}'}"`);
	});

	it("captures a browser event and persists it as a Pulse Event", () => {
		cy.login(E2E_USER, E2E_PASSWORD);
		cy.visit("/app/home");

		// Telemetry must be live in the browser, with direct-mode config from boot.
		cy.window().its("frappe.boot.enable_telemetry").should("eq", true);
		cy.window().its("frappe.boot.telemetry.host").should("eq", HOST);

		// The client loads asynchronously (dynamic import of the /assets script), so
		// wait until the provider has its client before firing.
		cy.window().should((win) => {
			const provider = win.frappe.telemetry?.providers?.find((p) => p.client);
			expect(provider, "pulse provider with loaded client").to.exist;
		});

		// Fire the event and force an immediate flush (skip the 10s timer / unload).
		cy.window().then((win) => {
			win.frappe.telemetry.capture(EVENT_NAME, "frappe", { source: "cypress" });

			const provider = win.frappe.telemetry.providers.find((p) => p.client);
			return provider.client.flush(); // resolves once the POST to bulk_ingest completes
		});

		// Drain the stream into the table by hand instead of waiting for the scheduler.
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

// CORS is owned by nginx (the site's `allow_cors`), so there's no app CORS to test.
// What pulse owns is auth: the key is accepted from the request body (browser path)
// or the X-Pulse-API-Key header (server-to-server). cy.request isn't browser-CORS-
// enforced, so it can exercise the endpoint directly.
context("Pulse ingest HTTP contract", () => {
	it("rejects ingest with a missing or wrong API key, accepts the right one", () => {
		// Missing key -> PermissionError (403).
		cy.request({
			method: "POST",
			url: INGEST_URL,
			failOnStatusCode: false,
			body: { events: [], site: SITE },
		}).then((res) => expect(res.status).to.eq(403));

		// Wrong key -> PermissionError (403).
		cy.request({
			method: "POST",
			url: INGEST_URL,
			failOnStatusCode: false,
			body: { events: [], site: SITE, api_key: "definitely-wrong" },
		}).then((res) => expect(res.status).to.eq(403));

		// Correct key in the body (browser path) -> accepted (empty batch, no cleanup).
		cy.request({
			method: "POST",
			url: INGEST_URL,
			body: { events: [], site: SITE, api_key: API_KEY },
		}).then((res) => expect(res.status).to.eq(200));

		// Correct key in the header (server-to-server path) -> also accepted.
		cy.request({
			method: "POST",
			url: INGEST_URL,
			headers: { "X-Pulse-API-Key": API_KEY },
			body: { events: [], site: SITE },
		}).then((res) => expect(res.status).to.eq(200));
	});
});
