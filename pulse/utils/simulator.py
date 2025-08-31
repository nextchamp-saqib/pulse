import random
import time

import frappe
from frappe.core.doctype.server_script.server_script import execute_api_server_script
from frappe.pulse.client import capture, send_queued_events
from frappe.utils.scheduler import get_scheduler_tick

# Base company names
COMPANY_NAMES = [
	"acme",
	"tech",
	"global",
	"retail",
	"healthcare",
	"financial",
	"logistics",
	"education",
	"consulting",
	"ecommerce",
	"construction",
	"restaurant",
	"medical",
	"legal",
	"marketing",
	"realestate",
	"accounting",
	"manufacturing",
	"shipping",
	"automotive",
	"energy",
	"pharma",
	"telecom",
	"banking",
	"insurance",
	"media",
	"hospitality",
	"agriculture",
	"aerospace",
	"chemicals",
	"textiles",
	"mining",
	"utilities",
	"transport",
	"wellness",
	"fitness",
	"beauty",
	"fashion",
	"furniture",
	"jewelry",
	"sports",
	"gaming",
	"publishing",
	"security",
	"cleaning",
	"catering",
	"photography",
	"design",
	"architecture",
	"engineering",
]

# Business suffixes
BUSINESS_SUFFIXES = [
	"corp",
	"inc",
	"group",
	"ltd",
	"company",
	"enterprises",
	"solutions",
	"services",
	"systems",
	"tech",
	"labs",
	"works",
	"industries",
	"partners",
	"associates",
	"world",
	"global",
	"international",
	"holdings",
	"ventures",
	"consulting",
	"agency",
	"studio",
	"center",
	"hub",
	"network",
	"collective",
	"alliance",
	"union",
	"co",
	"firm",
]

# Domain options for variety
DOMAINS = ["frappe.cloud", "frappecloud.com", "erpnext.com", "frappe.io"]

SITE_LIST = []


def generate_site_list():
	sites = set()

	while len(sites) < 8000:
		company = random.choice(COMPANY_NAMES)
		suffix = random.choice(BUSINESS_SUFFIXES)
		domain = random.choice(DOMAINS)

		# Create different name patterns for variety
		patterns = [
			f"{company}-{suffix}.{domain}",  # tech-corp.frappe.cloud
			f"{company}{suffix}.{domain}",  # techcorp.frappe.cloud
			f"{company}.{domain}",  # tech.frappe.cloud
			f"{suffix}-{company}.{domain}",  # corp-tech.frappe.cloud
		]

		site_name = random.choice(patterns)
		sites.add(site_name)

	return list(sites)


# Real Frappe app names
FRAPPE_APPS = [
	"erpnext",
	"hrms",
	"crm",
	"helpdesk",
	"lms",
	"wiki",
	"insights",
	"builder",
	"payments",
	"ecommerce_integrations",
	"webshop",
	"pos_awesome",
	"healthcare",
	"agriculture",
	"non_profit",
]

# Custom features that might be used
FEATURES = [
	"dashboard_view",
	"report_generation",
	"bulk_update",
	"email_send",
	"document_export",
	"workflow_approval",
	"notification_settings",
	"custom_script",
	"data_import",
	"backup_download",
	"user_permissions",
	"print_format",
	"integration_sync",
	"mobile_access",
	"api_call",
]


class SimplePulseSimulator:
	def __init__(self, config=None):
		self.config = frappe._dict(config or {})

	def _emit_heartbeat_event(self):
		"""Emit a simple app heartbeat event."""
		site = random.choice(SITE_LIST)
		app = random.choice(FRAPPE_APPS)

		capture(
			event_name="app_heartbeat",
			site=site,
			app=app,
			properties={
				"app_version": f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
			},
			interval="6h",
		)

	def _emit_custom_event(self):
		"""Emit a custom feature usage event."""
		site = random.choice(SITE_LIST)
		app = random.choice(FRAPPE_APPS)
		feature = random.choice(FEATURES)

		capture(
			event_name=f"{app}_{feature}",
			site=site,
			app=app,
			interval="15m",
		)

	def run_simulation(self):
		"""
		Run the simulation continuously, emitting events based on the configured rates.
		Steps:
		1. Check if simulation is enabled.
		2. Calculate the number of events
			- if events_per_minute is 30, and scheduler tick is 5 minutes,
			- total events to emit = 30 * 5 = 150
		3. For each event:
			- Emit a heartbeat event.
			- With a probability defined by custom_event_chance, emit a custom event.
		4. After emitting all events for this tick, send the queued events.
		"""
		if not self.config.enabled:
			return

		scheduler_tick_interval = int(get_scheduler_tick() / 60)
		for _ in range(self.config.events_per_minute * scheduler_tick_interval):
			self._emit_heartbeat_event()
			if random.random() < self.config.custom_event_chance:
				self._emit_custom_event()

		send_queued_events()


def run_scheduled_simulation():
	global SITE_LIST
	if not SITE_LIST:
		SITE_LIST = generate_site_list()

	config = frappe._dict({
		"enabled": False,
		"active_sites": 8000,
		"events_per_minute": 30,
		"custom_event_chance": 0.3,
	})

	try:
		script_doc = frappe.get_doc("Server Script", "pulse_simulation_config")
		out = execute_api_server_script(script_doc)
		if isinstance(out, dict) and out["config"]:
			config.update(out["config"])
	except Exception:
		config["enabled"] = False
		frappe.log_error(title="Pulse simulation config error")

	if not config.get("enabled"):
		return

	simulator = SimplePulseSimulator(config)
	simulator.run_simulation()
