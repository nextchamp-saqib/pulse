import "./commands";

// Desk throws assorted uncaught exceptions during navigation that are unrelated
// to what we're testing; don't let them fail the run.
Cypress.on("uncaught:exception", () => false);
