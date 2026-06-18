// Minimal harness so `bench --site <site> run-ui-tests pulse` can drive Pulse's
// own e2e specs. We export a plain object instead of using cypress's
// `defineConfig` helper: Cypress is installed in frappe's node_modules (where
// run-ui-tests puts it), not pulse's, so `require("cypress")` from this app root
// would fail. defineConfig is only an identity/type helper anyway.
//
// baseUrl/adminPassword are injected by run-ui-tests via CYPRESS_baseUrl /
// CYPRESS_adminPassword env vars; the values below are just defaults for
// `cypress open`.
module.exports = {
	adminPassword: "admin",
	testUser: "Administrator",
	defaultCommandTimeout: 20000,
	pageLoadTimeout: 15000,
	video: false,
	viewportHeight: 960,
	viewportWidth: 1400,
	retries: {
		runMode: 1,
		openMode: 0,
	},
	e2e: {
		setupNodeEvents(on) {
			on("before:browser:launch", (browser, launchOptions) => {
				if (browser.family === "chromium") {
					launchOptions.args.push("--disable-dev-shm-usage");
					launchOptions.args.push("--disable-gpu");
					launchOptions.args.push("--no-sandbox");
				}
				return launchOptions;
			});
		},
		testIsolation: false,
		baseUrl: "http://pulse.localhost:8000",
		supportFile: "cypress/support/e2e.js",
		specPattern: "cypress/integration/*.js",
	},
};
