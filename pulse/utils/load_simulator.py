import asyncio
import random
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import click
import frappe
from rich.align import Align
from rich.columns import Columns

# Rich imports for beautiful CLI
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Table
from rich.text import Text

from pulse.api import track_event

frappe.init(
	site="insights.localhost",
	sites_path="/Users/saqibansari/frappe/bench-v13/sites",
)
frappe.connect()

console = Console()


@dataclass
class SiteStats:
	site_id: int
	events_sent: int = 0
	last_event_time: float | None = None
	status: str = "Active"
	current_app: str = ""
	activity_level: float = 1.0


class SimulationConfig:
	def __init__(self):
		self.number_of_sites = 10
		self.apps = ["frappe", "erpnext", "gameplan", "hrms", "helpdesk"]

		# Timing configuration (in seconds)
		self.min_event_interval = 0.5
		self.max_event_interval = 5.0

		# Realism parameters
		self.burst_probability = 0.1
		self.burst_size_range = (2, 5)
		self.quiet_period_probability = 0.05
		self.quiet_duration_range = (10, 30)

		# Site behavior variation
		self.site_activity_variation = True
		self.peak_hours = (9, 17)
		self.timezone_simulation = True


class SiteSimulator:
	def __init__(self, site_id, config, stats_callback=None):
		self.site_id = site_id
		self.config = config
		self.stats_callback = stats_callback
		self.events_sent = 0
		self.is_active = True
		self.quiet_until = None
		self.current_status = "Starting"
		self.last_app = ""

		# Site characteristics
		self.activity_multiplier = random.uniform(0.3, 2.0)
		self.preferred_apps = self._select_preferred_apps()
		self.timezone_offset = random.randint(-12, 12)

		# Update stats callback
		if self.stats_callback:
			self.stats_callback(self.site_id, "activity_level", self.activity_multiplier)

	def _select_preferred_apps(self):
		apps = self.config.apps.copy()
		weights = [random.uniform(0.1, 1.0) for _ in apps]
		return list(zip(apps, weights, strict=False))

	def _get_weighted_app(self):
		apps, weights = zip(*self.preferred_apps, strict=False)
		return random.choices(apps, weights=weights)[0]

	def _is_peak_hours(self):
		if not self.config.timezone_simulation:
			return True
		current_hour = (datetime.now().hour + self.timezone_offset) % 24
		peak_start, peak_end = self.config.peak_hours

		if peak_start <= peak_end:
			return peak_start <= current_hour <= peak_end
		else:
			return current_hour >= peak_start or current_hour <= peak_end

	def _calculate_next_interval(self):
		base_min = self.config.min_event_interval
		base_max = self.config.max_event_interval

		min_interval = base_min / self.activity_multiplier
		max_interval = base_max / self.activity_multiplier

		if self._is_peak_hours():
			min_interval *= 0.5
			max_interval *= 0.7
		else:
			min_interval *= 1.5
			max_interval *= 2.0

		return random.uniform(min_interval, max_interval)

	async def _send_event(self):
		app_name = self._get_weighted_app()
		self.last_app = app_name

		try:
			track_event(
				site_name=f"site_{self.site_id}",
				event="app_pulse",
				app_name=app_name,
				app_version=f"1.{random.randint(0, 5)}.{random.randint(0, 10)}",
				timestamp=frappe.utils.now_datetime(),
			)
			self.events_sent += 1

			# Update stats
			if self.stats_callback:
				self.stats_callback(
					self.site_id,
					"event_sent",
					{"app": app_name, "total": self.events_sent, "timestamp": time.time()},
				)

			return True
		except Exception as e:
			if self.stats_callback:
				self.stats_callback(self.site_id, "error", str(e))
			return False

	async def _handle_burst_events(self):
		burst_size = random.randint(*self.config.burst_size_range)
		self.current_status = f"Burst ({burst_size})"

		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

		for i in range(burst_size):
			await self._send_event()
			if i < burst_size - 1:
				await asyncio.sleep(random.uniform(0.1, 0.5))

		self.current_status = "Active"

	async def _enter_quiet_period(self):
		quiet_duration = random.randint(*self.config.quiet_duration_range)
		self.quiet_until = time.time() + quiet_duration
		self.current_status = f"Quiet ({quiet_duration}s)"

		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

	async def simulate(self):
		self.current_status = "Active"
		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

		while self.is_active:
			current_time = time.time()

			# Check if in quiet period
			if self.quiet_until and current_time < self.quiet_until:
				await asyncio.sleep(1)
				continue
			else:
				if self.current_status.startswith("Quiet"):
					self.current_status = "Active"
					if self.stats_callback:
						self.stats_callback(self.site_id, "status", self.current_status)
				self.quiet_until = None

			# Decide what to do this iteration
			rand = random.random()

			if rand < self.config.quiet_period_probability:
				await self._enter_quiet_period()
			elif rand < self.config.quiet_period_probability + self.config.burst_probability:
				await self._handle_burst_events()
			else:
				# Normal single event
				await self._send_event()

			# Wait for next event
			interval = self._calculate_next_interval()
			await asyncio.sleep(interval)


class TelemetryDashboard:
	def __init__(self, config):
		self.config = config
		self.sites_stats = {}
		self.total_events = 0
		self.start_time = time.time()
		self.throughput_history = deque(maxlen=20)  # Last 20 measurements
		self.app_stats = defaultdict(int)
		self.last_update = time.time()

		# Initialize site stats
		for i in range(1, config.number_of_sites + 1):
			self.sites_stats[i] = SiteStats(site_id=i)

	def update_stats(self, site_id, event_type, data):
		"""Callback function for site simulators to update stats"""
		if site_id not in self.sites_stats:
			self.sites_stats[site_id] = SiteStats(site_id=site_id)

		site_stats = self.sites_stats[site_id]

		if event_type == "event_sent":
			site_stats.events_sent = data["total"]
			site_stats.last_event_time = data["timestamp"]
			site_stats.current_app = data["app"]
			self.total_events += 1
			self.app_stats[data["app"]] += 1
		elif event_type == "status":
			site_stats.status = data
		elif event_type == "activity_level":
			site_stats.activity_level = data

	def calculate_throughput(self):
		"""Calculate current throughput"""
		current_time = time.time()
		if current_time - self.last_update >= 1.0:  # Update every second
			elapsed = current_time - self.start_time
			if elapsed > 0:
				current_throughput = self.total_events / elapsed
				self.throughput_history.append(current_throughput)
				self.last_update = current_time

	def create_overview_panel(self):
		"""Create the main overview panel"""
		elapsed = time.time() - self.start_time

		# Calculate stats
		active_sites = sum(1 for s in self.sites_stats.values() if s.status != "Quiet")
		avg_throughput = self.total_events / elapsed if elapsed > 0 else 0

		# Recent throughput
		recent_throughput = self.throughput_history[-1] if self.throughput_history else 0

		overview_text = f"""
[bold cyan]🚀 Telemetry Simulation Dashboard[/bold cyan]

[green]Runtime:[/green] {elapsed:.0f}s
[green]Total Events:[/green] {self.total_events:,}
[green]Active Sites:[/green] {active_sites}/{self.config.number_of_sites}
[green]Avg Throughput:[/green] {avg_throughput:.2f} events/sec
[green]Current Rate:[/green] {recent_throughput:.2f} events/sec
        """.strip()

		return Panel(overview_text, title="📊 Overview", border_style="cyan")

	def create_sites_table(self):
		"""Create the sites status table"""
		table = Table(title="🌐 Site Status", show_header=True, header_style="bold magenta")
		table.add_column("Site", style="cyan", no_wrap=True)
		table.add_column("Status", style="green")
		table.add_column("Events", justify="right", style="blue")
		table.add_column("Last App", style="yellow")
		table.add_column("Activity", justify="right", style="red")
		table.add_column("Last Event", style="dim")

		# Sort by site_id
		sorted_sites = sorted(self.sites_stats.items())

		for site_id, stats in sorted_sites:
			# Status styling
			status_style = (
				"green" if stats.status == "Active" else "yellow" if "Burst" in stats.status else "red"
			)

			# Last event timing
			if stats.last_event_time:
				last_event = f"{time.time() - stats.last_event_time:.1f}s ago"
			else:
				last_event = "Never"

			table.add_row(
				f"Site {site_id}",
				f"[{status_style}]{stats.status}[/{status_style}]",
				f"{stats.events_sent:,}",
				stats.current_app or "-",
				f"{stats.activity_level:.1f}x",
				last_event,
			)

		return table

	def create_app_stats_panel(self):
		"""Create app usage statistics"""
		if not self.app_stats:
			return Panel("No data yet...", title="📱 App Usage")

		total = sum(self.app_stats.values())
		stats_text = []

		# Sort apps by usage
		sorted_apps = sorted(self.app_stats.items(), key=lambda x: x[1], reverse=True)

		for app, count in sorted_apps:
			percentage = (count / total * 100) if total > 0 else 0
			bar_length = int(percentage / 5)  # Scale bar
			bar = "█" * bar_length + "░" * (20 - bar_length)
			stats_text.append(f"{app:10} {bar} {count:4d} ({percentage:4.1f}%)")

		return Panel("\n".join(stats_text), title="📱 App Usage", border_style="yellow")

	def create_throughput_graph(self):
		"""Create a simple ASCII throughput graph"""
		if len(self.throughput_history) < 2:
			return Panel("Collecting data...", title="📈 Throughput History")

		# Simple ASCII graph
		max_val = max(self.throughput_history) if self.throughput_history else 1
		graph_lines = []

		for i, value in enumerate(list(self.throughput_history)[-10:]):  # Last 10 points
			bar_height = int((value / max_val) * 10) if max_val > 0 else 0
			bar = "█" * bar_height + "░" * (10 - bar_height)
			graph_lines.append(f"{i:2d}: {bar} {value:5.2f}")

		return Panel("\n".join(graph_lines), title="📈 Throughput (events/sec)", border_style="green")

	def create_layout(self):
		"""Create the complete dashboard layout"""
		self.calculate_throughput()

		# Create layout components
		overview_panel = self.create_overview_panel()
		app_stats_panel = self.create_app_stats_panel()
		throughput_panel = self.create_throughput_graph()
		sites_table = self.create_sites_table()

		# Create layout
		layout = Layout()

		layout.split_column(
			Layout(overview_panel, size=8),
			Layout(name="middle", ratio=2),
			Layout(sites_table, name="bottom", size=12),
		)

		layout["middle"].split_row(Layout(app_stats_panel), Layout(throughput_panel))

		return layout


class TelemetrySimulation:
	def __init__(self, config):
		self.config = config
		self.dashboard = TelemetryDashboard(config)
		self.sites = []
		self.is_running = False

	async def run_simulation(self):
		"""Run the simulation with live dashboard"""
		# Initialize sites with dashboard callback
		self.sites = [
			SiteSimulator(i, self.config, self.dashboard.update_stats)
			for i in range(1, self.config.number_of_sites + 1)
		]

		# Create site simulation tasks with proper coroutine handling
		async def create_site_task(site_instance, delay):
			await asyncio.sleep(delay)
			await site_instance.simulate()

		# Create tasks for all sites with staggered startup
		site_tasks = []
		for i, site in enumerate(self.sites):
			startup_delay = random.uniform(0, 5)
			task = create_site_task(site, startup_delay)
			site_tasks.append(task)

		self.is_running = True

		# Start simulation with live dashboard
		with Live(self.dashboard.create_layout(), refresh_per_second=2, screen=True) as live:
			try:
				# Update dashboard in background
				async def update_dashboard():
					while self.is_running:
						try:
							live.update(self.dashboard.create_layout())
						except Exception:
							# Handle any layout update errors gracefully
							pass
						await asyncio.sleep(0.5)  # Update twice per second

				# Run dashboard updater and all site simulations
				await asyncio.gather(update_dashboard(), *site_tasks, return_exceptions=True)

			except KeyboardInterrupt:
				console.print("\n[red]Simulation interrupted by user[/red]")
			finally:
				self.is_running = False

				# Final summary
				console.print("\n" + "=" * 60)
				console.print("[bold green]📊 Final Summary[/bold green]")
				console.print(f"Total Events: {self.dashboard.total_events:,}")
				console.print(f"Runtime: {time.time() - self.dashboard.start_time:.1f}s")
				avg_throughput = self.dashboard.total_events / (time.time() - self.dashboard.start_time)
				console.print(f"Average Throughput: {avg_throughput:.2f} events/sec")
				console.print("=" * 60)


@click.command()
@click.option("--sites", default=10, help="Number of sites to simulate")
@click.option("--min-interval", default=0.5, help="Minimum event interval (seconds)")
@click.option("--max-interval", default=5.0, help="Maximum event interval (seconds)")
@click.option("--apps", default="frappe,erpnext,gameplan,hrms,helpdesk", help="Comma-separated list of apps")
def main(sites, min_interval, max_interval, apps):
	"""🚀 Realistic Telemetry Simulation with Live Dashboard

	Press Ctrl+C to stop the simulation at any time.
	"""
	console.print("[bold green]🚀 Starting Telemetry Simulation...[/bold green]")

	# Update config based on CLI args
	config = SimulationConfig()
	config.number_of_sites = sites
	config.min_event_interval = min_interval
	config.max_event_interval = max_interval
	config.apps = [app.strip() for app in apps.split(",")]

	console.print(f"Configuration: {sites} sites, {len(config.apps)} apps")
	console.print("[dim]Starting in 3 seconds... Press Ctrl+C to stop anytime[/dim]")
	time.sleep(3)

	simulation = TelemetrySimulation(config)

	try:
		asyncio.run(simulation.run_simulation())
	except KeyboardInterrupt:
		console.print("\n[yellow]Shutting down gracefully...[/yellow]")


if __name__ == "__main__":
	main()
