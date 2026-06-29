# Pulse on the Frappe stack

Pulse is the product-analytics service. Apps send it **events**; Pulse stores them;
funnels, activation, and retention are computed from them later in Insights.

This document follows one account through its whole lifecycle on the Frappe stack —
anonymous visitor → signup → provisioned site → engaged user → activated → retained —
and shows, at each step, what is sent and how it is wired up. It is an example, not a
spec: event names and milestones are illustrative.

**Contents**

- [Model](#model)
- [The flow](#the-flow)
    - [1. Product website](#1-product-website)
    - [2. Signup](#2-signup)
    - [3. The site](#3-the-site)
    - [4. Activation and retention](#4-activation-and-retention)
- [Configuration](#configuration)
- [APIs](#apis)
- [Internals](#internals)

## Model

Every event carries two ids:

- **`user`** — one person. Anonymous visitors get a client-minted `anon_…` id; known
  users get `user_…`, a salted hash of the account user (never the raw email).
- **`team`** — the account (a Frappe Cloud team). Stable for the account's whole life.

A person's `user` id changes as they move between contexts — a marketing site, the
signup app, their own site. `team` does not. Cross-stage analysis is done on `team`.

One distinction is worth stating up front: you **send events**, and you **derive**
funnels, activation, and retention. The latter are not events; they are questions
asked of the event history.

### A note on `user` scope

`user` is **scope-dependent**, and the same column name carries three different
kinds of id over the lifecycle:

- `anon_…` — a browser, on the marketing site
- `user_…` — the FC account user, minted at signup (`identify`/`alias` target)
- `user_…` — a **per-site-salted** hash of the site user, on each site

The last two render identically but are **not comparable**: the salt is per-site by
design (privacy/tenancy — a person id never leaves a site in a cross-site-linkable
form), so the same human is a different `user_…` on every site, and the account
`user_…` you `identify()` at signup never appears on the site at all. The two are
linked only by `team`.

Two consequences for anyone querying events:

- The safe key for site-scoped, per-person analysis is **`(site, user)`** — `site`
  is on every event. A bare `COUNT(DISTINCT user)` across sites silently counts
  *human × site*, not humans.
- **`team` is the only identity stable across stages.** Anonymous → signup → site
  activity is joined on `team`, never on `user`. Make `team` the cross-stage join;
  treat `user` as valid only within a single `site`.

`team` and `user` aren't in tension — they're orthogonal axes (the account vs. the
actor within it). The only hazard is reading a site-scoped `user` as if it were
global; key by `(site, user)` and join cross-stage on `team` and it goes away.

## The flow

### 1. Product website

`frappe.io`, `erpnext.com`, and the like. Visitors are anonymous.

The browser client is loaded once and given a public, write-only key. By default it
runs **cookieless** (the Plausible model): it writes **nothing** to the browser and
mints no id. With no user in its config it sends events with no `user`, and the host
derives one at ingest as `sha256(daily_salt + site + ip + user_agent)`, where
`daily_salt` is a server-side secret that rotates every UTC day and the previous day's
salt is discarded:

```js
import { PulseClient } from "https://pulse.m.frappe.cloud/assets/pulse/js/pulse_client.js";

const pulse = new PulseClient({
  host: "https://pulse.m.frappe.cloud",
  apiKey: PULSE_PUBLIC_KEY, // public, write-only — safe in the browser
  site: "frappe.io",
  enabled: true,
});
pulse.init();

pulse.capture("pricing_viewed", "website", { plan: "erpnext" });
```

```
pageview        user=anon_3f1b…   team=—
pricing_viewed  user=anon_3f1b…   team=—
```

So a visitor is one stable id *within* a UTC day and a different id the next —
cross-day anonymous attribution is intentionally dropped. Same-session signup still
stitches: `getDistinctId()` returns the current day's derived id, so the click-time
`aid` forward (below) carries it into `alias()` unchanged.

**Stored-id mode (opt-out: `anonymous_mode: "client"`).** A site that needs a browser
to stay one identity *across* days can instead mint a persistent `anon_…` id in
localStorage — at the cost of the storage consent it needs in most jurisdictions,
which is why cookieless is the default. Stages 2–4 are identical in both modes.

### 2. Signup

`cloud.frappe.io/signup/erpnext`. localStorage does not cross domains, so the product
site forwards the anonymous id on the outbound link:

```js
const signup = `https://cloud.frappe.io/signup/erpnext?aid=${pulse.getDistinctId()}`;
```

On submit, Frappe Cloud creates a user and a team, then stitches and labels the new
identity from the server:

```python
from frappe.utils.telemetry.pulse.client import alias, identify

alias(previous_id=anon_id, user=user_id)                    # same person
identify(user_id, {"plan": "trial", "product": "erpnext"})  # facts about them
```

- **`alias`** links two ids — the anonymous browsing above is now attributed to
  `user_id`.
- **`identify`** attaches attributes to a person; it does not link ids.

Both run server-side only: they change identity state, so they are never reachable
from a browser. From here the team exists and rides on every later event.

### 3. The site

`myerpnext.frappe.cloud`. At provisioning the team id is written into the site's
config as `fc_team`, and the telemetry layer stamps it on every event — from both
clients, with no lookup needed.

Two clients send events: same shape, different vantage points.

- The **browser client** captures in-page actions. On desk and SPAs it reads its
  config from `boot.telemetry` rather than being constructed by hand.
- The **server client** captures what the backend is authoritative about.

```python
import frappe

# user (anonymized session user) and team (from site config) are filled in for you
frappe.utils.telemetry.capture(
    "document_created", "erpnext", properties={"doctype": "Sales Order"}
)
```

On a site, `user` is the site's own user, anonymized with a per-site salt — so the
same human is a different `user_…` on each site, and `team` is what ties them
together.

```
site_created      user=user_7c1d   team=team_x   (server)
document_created  user=user_7c1d   team=team_x   (server/browser)
report_viewed     user=user_9a30   team=team_x
```

Different site users are different `user` ids under one `team`, so both per-user and
per-account views fall out of the same events.

### 4. Activation and retention

Neither is sent — both are computed from the events above.

- **Activation** is a milestone defined over events, e.g. "10+ documents across 3+
  doctypes in the first week." Instrumentation only emits the underlying events with
  enough properties; the definition lives in Insights and can change without code.
- **Retention** groups accounts by signup cohort and checks for qualifying activity in
  later periods, counted at the `team` level — the identity that persists across an
  account's users and sites.

## Configuration

| Where         | Setting          | Purpose                                                         |
| ------------- | ---------------- | --------------------------------------------------------------- |
| site config   | `pulse_api_key`  | public, write-only ingest key                                   |
| site config   | `pulse_host`     | ingest host (default `https://pulse.m.frappe.cloud`)            |
| site config   | `fc_team`        | team id, written at provisioning; stamped on events             |
| site config   | `anonymous_mode` | `cookieless` (default) or `client` — anonymous id strategy (§1) |
| browser / SPA | `boot_config()`  | serves the client its host, key, site, user, team               |

Telemetry stays off unless a key is set and telemetry is enabled; a disabled site
hands the browser nothing.

## APIs

| Call                                 | Side             | Does                                          |
| ------------------------------------ | ---------------- | --------------------------------------------- |
| `capture(event, app, properties, …)` | browser + server | record an event                               |
| `identify(user, properties)`         | server only      | set/merge attributes on a person              |
| `alias(previous_id, user)`           | server only      | record that an anonymous id is a known person |
| `boot_config()`                      | server (guest)   | hand the browser its client config            |

`alias` only ever merges an **anonymous** id into a known one; it refuses to collapse
two already-known identities.

## Internals

Brief notes — the code docstrings carry the detail.

- **Direct-to-host ingest.** Browsers post events straight to the Pulse host
  (form-encoded, key in the body — a preflight-free request). There is no relay
  through the app server.
- **One shared client.** `pulse_client.js` is served by Pulse and loaded at runtime by
  desk, SPAs, and web pages; nothing vendors a copy.
- **Queue and retry.** Server events buffer in Redis and drain in batches with
  retry/backoff; the browser client batches and flushes on an interval and on page
  hide.
- **Anonymization.** `user_…` is a per-site-salted SHA-256 of the user. Standard users
  (Guest, Administrator) are never sent. By default (`anonymous_mode: "cookieless"`) the
  anonymous `user` is derived at ingest from `daily_salt + site + ip + user_agent` — no
  client id, no storage. The salt is global to the Pulse server and rotates daily, but
  `site` is in the hash, so the same browser is still a distinct id per site and per day
  (`pulse/anon.py`). Under the opt-out `client` mode a guest instead falls back to a
  browser-minted `anon_…` id.
- **Id namespaces.** `anon_…` (client-minted, per browser) · `user_…` (server, per
  site) · `team_…` (account).
