// The browser pulse client, and the single source of truth for it. Served by
// pulse as a static asset at /assets/pulse/js/pulse_client.js (nginx adds the CORS
// header); desk, frappe-ui apps, and plain web pages all load it from there at
// runtime, so it does NOT depend on the host site's framework version. Nothing
// else vendors a copy.
//
// It posts events *directly* to the pulse ingest service. Config
// is injected by the host (desk reads it from boot.telemetry; frappe-ui / press
// pass their own). The ingest key is a public, write-only key — exposing it in
// the browser is by design (all pulse endpoints are POST-only writes).
//
// No CSRF, no is_enabled round-trip, no cookies. Events are posted as a "simple"
// cross-origin request — form-encoded body, key in the body, no custom headers —
// so the browser skips the CORS preflight entirely. Pulse's CORS is then just the
// Access-Control-Allow-Origin nginx puts on the response (via the site's
// `allow_cors`); there's nothing for the app to do per-request.

const INGEST_PATH = "/api/method/pulse.api.bulk_ingest";

function defaultContext() {
	const t = (typeof window !== "undefined" && window.frappe?.boot?.telemetry) || {};
	return { user: t.user, team: t.team };
}

export class PulseClient {
	constructor(options = {}) {
		const {
			host,
			apiKey,
			site,
			enabled = false,
			getContext,
			flushInterval = 10000,
			maxQueueSize = 20,
			now,
		} = options;

		this.host = (host || "").replace(/\/+$/, "");
		this.apiKey = apiKey;
		this.site =
			site || (typeof window !== "undefined" ? window.location?.hostname : undefined);
		this.getContext = getContext || defaultContext;
		this.flushInterval = flushInterval;
		this.maxQueueSize = maxQueueSize;
		this.now = now || (() => new Date().toISOString());

		// Direct mode needs a host + key to send anything; without them stay off.
		this.enabled = Boolean(enabled && this.host && this.apiKey);
		this.eq = null;
		this.unloadAttached = false;
	}

	// Kept for API compatibility with the old relay client (callers awaited it).
	// There's no backend round-trip anymore — enablement is decided from config.
	async init() {
		if (this.enabled) this.start();
		return this.enabled;
	}

	setEnabled(enabled) {
		this.enabled = Boolean(enabled && this.host && this.apiKey);
		if (!this.enabled) this.stop();
	}

	start() {
		if (!this.enabled || this.eq) return;
		this.eq = new QueueManager((events) => this._send(events), {
			flushInterval: this.flushInterval,
			maxQueueSize: this.maxQueueSize,
		});
		this._attachUnload();
	}

	capture(event_name, app, props) {
		if (!this.enabled) return;
		if (!this.eq) this.start();

		const { user, team } = this.getContext() || {};
		this.eq.add({
			event_name: event_name,
			app: app,
			properties: props,
			site: this.site,
			user: user,
			team: team,
			captured_at: this.now(),
		});
	}

	flush() {
		return this.eq?.flush();
	}

	stop() {
		this.eq?.stop();
		this.eq = null;
	}

	_send(events) {
		// Form-encoded body + key in the body + no custom headers => a "simple"
		// CORS request, so no preflight. (URLSearchParams makes fetch send
		// Content-Type: application/x-www-form-urlencoded, which is CORS-safelisted.)
		const body = new URLSearchParams({
			events: JSON.stringify(events),
			site: this.site,
			api_key: this.apiKey,
		});
		return fetch(`${this.host}${INGEST_PATH}`, {
			method: "POST",
			credentials: "omit",
			keepalive: true,
			body: body,
		}).then((r) => {
			if (!r.ok) throw new Error(`pulse ingest failed: ${r.status}`);
		});
	}

	// On unload, send whatever is buffered in one keepalive fetch. `sendBeacon`
	// can't set the X-Pulse-API-Key header, so it's not an option here — keepalive
	// fetch is what survives the page going away.
	_attachUnload() {
		if (this.unloadAttached || typeof window === "undefined") return;
		this.unloadAttached = true;

		const flushBuffered = () => {
			const events = this.eq?.getBufferedEvents?.() || [];
			if (events.length) this._send(events);
		};

		window.addEventListener("pagehide", flushBuffered);
		document.addEventListener("visibilitychange", () => {
			if (document.visibilityState === "hidden") flushBuffered();
		});
	}
}

class QueueManager {
	constructor(flushCallback, options = {}) {
		this.flushCallback = flushCallback;
		this.queue = [];
		this.pendingBatch = null;
		this.retryAttempts = 0;
		this.maxRetries = 3;
		this.maxQueueSize = options.maxQueueSize || 20;
		this.flushInterval = options.flushInterval || 5000;
		this.timer = null;
		this.flushing = false;

		this.start();
	}

	getBufferedEvents() {
		const events = [];
		if (this.pendingBatch?.length) events.push(...this.pendingBatch);
		if (this.queue.length) events.push(...this.queue);
		return events;
	}

	start() {
		this.timer = setInterval(() => {
			if (this.queue.length || this.pendingBatch) this.flush();
		}, this.flushInterval);
	}

	add(event) {
		this.queue.push(event);

		if (this.queue.length >= this.maxQueueSize) {
			this.flush();
		}
	}

	async flush() {
		if (this.flushing) return;
		this.flushing = true;

		try {
			if (!this.pendingBatch) {
				if (!this.queue.length) return;
				this.pendingBatch = this.queue.splice(0, this.maxQueueSize);
				this.retryAttempts = 0;
			}

			try {
				await this.flushCallback(this.pendingBatch);
				this.pendingBatch = null;
				this.retryAttempts = 0;
			} catch (error) {
				this.retryAttempts++;
				if (this.retryAttempts > this.maxRetries) {
					this.pendingBatch = null;
					this.retryAttempts = 0;
				}
			}
		} finally {
			this.flushing = false;
		}
	}

	stop() {
		if (this.timer) {
			clearInterval(this.timer);
			this.timer = null;
		}
		this.flush();
	}
}

if (typeof window !== "undefined") {
	window.frappe = window.frappe || {};
	window.frappe.PulseClient = PulseClient;
}
