### Pulse

A Frappe app to track user interactions with your product. Events are posted
directly from the browser to Pulse (and server-side from the framework's telemetry
client), buffered through Redis, and persisted as `Pulse Event` records for
analysis.

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

`pulse_host` is where the browser both loads the client
(`/assets/pulse/js/pulse_client.js`) and posts events; `pulse_api_key` is sent with
each event and must match **Pulse Settings → API Key**. On a local bench
`is_enabled()` is off (dev mode / not Frappe Cloud); `pulse.e2e.setup` flips
`pulse_force_enabled`, syncs the key into Pulse Settings, and points the client at
the local host for you:

```bash
bench --site $SITE execute pulse.e2e.setup
```

Then rebuild assets so Desk picks up the telemetry provider, trigger activity
(create a doc, navigate Desk), drain the queues, and watch rows land — see the
quick check below. **`pulse_force_enabled` is test-only; never set it in
production.**

```bash
bench build --app frappe
```

The local setup is same-origin (the browser and Pulse share `$SITE`), so no CORS
is involved. A real cross-origin setup (browser on a product site posting to a
separate Pulse host) needs `allow_cors` set on the Pulse site, so nginx adds the
`Access-Control-Allow-Origin` header on `/assets` and the ingest response.

```bash
bench --site $SITE set-config allow_cors "*"
```

### Testing

An end-to-end test proves the full pipeline across the browser client and this app.
The browser posts directly to Pulse (no framework relay):

```
browser frappe.telemetry.capture()  [pulse_client.js from /assets/pulse/]
  -> POST bulk_ingest  ->  Pulse Redis stream
  -> consume_pulse_events()  ->  Pulse Event row
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

CI runs the e2e on every PR (`.github/workflows/e2e.yml`). The framework-side
telemetry changes (boot config + direct-mode provider in `pulse.js`) currently live
on frappe's `improve-pulse-telemetry` branch, so CI installs that via
`FRAPPE_BRANCH` — switch it to `develop` once they merge.

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
