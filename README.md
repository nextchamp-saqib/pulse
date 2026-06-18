### Pulse

A Frappe app to track user interactions with your product. Events are sent by the
framework's telemetry client, buffered through Redis, and persisted as `Pulse Event`
records for analysis.

### Installation

You can install this app using the [bench](https://github.com/frappe/bench) CLI:

```bash
cd $PATH_TO_YOUR_BENCH
bench get-app $URL_OF_THIS_REPO --branch develop
bench install-app pulse
```

### Manual setup

To send real telemetry to a local Pulse from a bench, point the framework client
at your Pulse site and give it a matching key:

```bash
# in the site that should emit events (can be the Pulse site itself)
bench --site $SITE set-config pulse_host "http://$SITE:8000"
bench --site $SITE set-config pulse_api_key "asdf1234"
```

Set the same key in **Pulse Settings** (so ingestion accepts it) and enable
telemetry. On a local bench `is_enabled()` is off (dev mode / not Frappe Cloud);
`pulse.e2e.setup` flips the `pulse_force_enabled` conf flag and syncs the key for
you:

```bash
bench --site $SITE execute pulse.e2e.setup
```

Then trigger activity (create a doc, navigate Desk), drain the queues, and watch
rows land — see the quick check below. **`pulse_force_enabled` is test-only;
never set it in production.**

### Testing

An end-to-end test proves the full pipeline across the framework client and this app:

```
browser frappe.telemetry.capture() -> bulk_capture -> client Redis queue
  -> send_queued_events() -> bulk_ingest -> Pulse Redis stream
  -> consume_pulse_events() -> Pulse Event row
```

Run it against a site that has both `frappe` and `pulse` installed (e.g. one with
`pulse_api_key` / `pulse_host` pointing back at itself):

```bash
bench --site $SITE run-ui-tests pulse --headless \
  --spec cypress/integration/pulse_telemetry.js
```

The test calls `pulse.e2e.setup` itself, so no manual prep is needed.

Quick server-only check, no browser:

```bash
bench --site $SITE execute pulse.e2e.setup
bench --site $SITE execute frappe.utils.telemetry.pulse.client.capture \
  --kwargs "{'event_name':'cypress_pulse_e2e','app':'frappe'}"
bench --site $SITE execute pulse.e2e.drain
bench --site $SITE execute pulse.e2e.count   # -> count=1
bench --site $SITE execute pulse.e2e.cleanup
```

CI runs the e2e on every PR (`.github/workflows/e2e.yml`). The telemetry client
currently lives on frappe's `improve-pulse-telemetry` branch, so CI installs that
via `FRAPPE_BRANCH` — switch it to `develop` once the client merges.

### Contributing

This app uses `pre-commit` for code formatting and linting. Please [install pre-commit](https://pre-commit.com/#installation) and enable it for this repository:

```bash
cd apps/pulse
pre-commit install
```

Pre-commit is configured to use the following tools for checking and formatting your code:

- ruff
- eslint
- prettier
- pyupgrade

### License

agpl-3.0
