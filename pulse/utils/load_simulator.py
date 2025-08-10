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
	site="pulse.localhost",
	sites_path="/Users/saqibansari/frappe/bench-v13/sites",
)
frappe.connect()

console = Console()


@dataclass
class SiteStats:
	site_id: int
	events_sent: int = 0
	last_event_time: float | None = None
	status: str = "Starting"
	current_app: str = ""
	activity_level: float = 1.0


class SimulationConfig:
	def __init__(self):
		self.number_of_sites = 5000  # 5000 sites
		self.apps = ["frappe", "erpnext", "gameplan", "hrms", "helpdesk"]

		# Timing configuration - adjusted for 0.5 events/sec total across all sites
		# With 5000 sites, each site should send ~0.0001 events/sec on average
		# This means roughly 1 event per site every ~2.8 hours (10,000 seconds)
		self.min_event_interval = 8000  # Minimum 8000 seconds (~2.2 hours) between events
		self.max_event_interval = 12000  # Maximum 12000 seconds (~3.3 hours) between events

		# Realism parameters - reduced probabilities for less frequent special events
		self.burst_probability = 0.001  # Very rare bursts (0.1%)
		self.burst_size_range = (2, 3)  # Smaller bursts
		self.quiet_period_probability = 0.01  # 1% chance of quiet period
		self.quiet_duration_range = (3600, 7200)  # 1-2 hour quiet periods

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

		# Site characteristics
		self.activity_multiplier = random.uniform(0.1, 3.0)  # Wider range for more variation
		self.preferred_apps = self._select_preferred_apps()
		self.timezone_offset = random.randint(-12, 12)

		# Update stats callback
		if self.stats_callback:
			self.stats_callback(self.site_id, "activity_level", self.activity_multiplier)
			self.stats_callback(self.site_id, "status", "Starting")

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
			min_interval *= 0.7  # Slightly more frequent during peak hours
			max_interval *= 0.8
		else:
			min_interval *= 1.3  # Less frequent during off-hours
			max_interval *= 1.5

		return random.uniform(min_interval, max_interval)

	async def _send_event(self):
		app_name = self._get_weighted_app()

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
		self.current_status = f"Burst({burst_size})"

		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

		for i in range(burst_size):
			await self._send_event()
			if i < burst_size - 1:
				await asyncio.sleep(random.uniform(60, 300))  # 1-5 minutes between burst events

		self.current_status = "Active"
		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

	async def _enter_quiet_period(self):
		quiet_duration = random.randint(*self.config.quiet_duration_range)
		self.quiet_until = time.time() + quiet_duration
		self.current_status = f"Quiet({quiet_duration // 3600}h)"

		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

	async def simulate(self):
		# Initial random delay to spread out the startup
		initial_delay = random.uniform(0, 3600)  # Up to 1 hour spread
		await asyncio.sleep(initial_delay)

		self.current_status = "Active"
		if self.stats_callback:
			self.stats_callback(self.site_id, "status", self.current_status)

		while self.is_active:
			current_time = time.time()

			# Check if in quiet period
			if self.quiet_until and current_time < self.quiet_until:
				await asyncio.sleep(60)  # Check every minute during quiet period
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
		self.total_events = 0
		self.start_time = time.time()
		self.throughput_history = deque(maxlen=60)  # Last 60 measurements
		self.app_stats = defaultdict(int)
		self.last_throughput_calculation = time.time()

		# Site status counters (instead of tracking individual sites)
		self.site_status_counts = {
			"Starting": config.number_of_sites,  # All sites start as "Starting"
			"Active": 0,
			"Burst": 0,
			"Quiet": 0,
		}

		# Site activity distribution
		self.activity_distribution = defaultdict(int)

		# Recent events tracking for better throughput calculation
		self.recent_events = deque(maxlen=200)

		# Error tracking
		self.error_count = 0

	def update_stats(self, site_id, event_type, data):
		"""Callback function for site simulators to update stats"""
		current_time = time.time()

		if event_type == "event_sent":
			self.total_events += 1
			self.app_stats[data["app"]] += 1
			self.recent_events.append(current_time)

		elif event_type == "status":
			# For performance, only update status counts when status actually changes
			# We don't track individual sites anymore, just the counts
			if data == "Active":
				self.site_status_counts["Starting"] = max(0, self.site_status_counts["Starting"] - 1)
				self.site_status_counts["Active"] += 1
			elif data.startswith("Burst"):
				self.site_status_counts["Active"] = max(0, self.site_status_counts["Active"] - 1)
				self.site_status_counts["Burst"] += 1
			elif data.startswith("Quiet"):
				# Could be transitioning from Active or Burst
				if self.site_status_counts["Active"] > 0:
					self.site_status_counts["Active"] -= 1
				elif self.site_status_counts["Burst"] > 0:
					self.site_status_counts["Burst"] -= 1
				self.site_status_counts["Quiet"] += 1
			# When leaving Burst or Quiet, they go back to Active (handled above)

		elif event_type == "activity_level":
			# Track activity distribution in buckets
			activity_bucket = round(data * 2) / 2  # Round to nearest 0.5
			self.activity_distribution[f"{activity_bucket:.1f}x"] += 1

		elif event_type == "error":
			self.error_count += 1

	def calculate_throughput(self):
		"""Calculate current throughput with better accuracy for low-frequency events"""
		current_time = time.time()

		# Calculate throughput every 30 seconds for better accuracy at very low rates
		if current_time - self.last_throughput_calculation >= 30.0:
			elapsed_total = current_time - self.start_time

			# Overall average throughput
			avg_throughput = self.total_events / elapsed_total if elapsed_total > 0 else 0

			# Recent throughput (events in last 5 minutes for low-frequency events)
			recent_cutoff = current_time - 300  # 5 minutes
			recent_count = sum(1 for event_time in self.recent_events if event_time > recent_cutoff)
			recent_throughput = recent_count / 300.0  # Events per second over 5 minutes

			self.throughput_history.append(recent_throughput)
			self.last_throughput_calculation = current_time

			return avg_throughput, recent_throughput

		# Return last calculated values or defaults
		elapsed_total = current_time - self.start_time
		avg_throughput = self.total_events / elapsed_total if elapsed_total > 0 else 0
		recent_throughput = self.throughput_history[-1] if self.throughput_history else 0
		return avg_throughput, recent_throughput

	def create_overview_panel(self):
		"""Create the main overview panel"""
		elapsed = time.time() - self.start_time
		avg_throughput, recent_throughput = self.calculate_throughput()

		# Format time nicely
		hours = int(elapsed // 3600)
		minutes = int((elapsed % 3600) // 60)
		seconds = int(elapsed % 60)
		time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

		# Calculate next event estimate
		if recent_throughput > 0:
			next_event_est = int(1.0 / recent_throughput)
			if next_event_est > 3600:
				next_event_str = f"~{next_event_est // 3600}h {(next_event_est % 3600) // 60}m"
			elif next_event_est > 60:
				next_event_str = f"~{next_event_est // 60}m {next_event_est % 60}s"
			else:
				next_event_str = f"~{next_event_est}s"
		else:
			next_event_str = "Calculating..."

		# Color code the throughput based on target
		target = 0.5
		throughput_color = "green" if abs(recent_throughput - target) < 0.1 else "yellow"

		overview_text = f"""
[bold cyan]🚀 Large Scale Telemetry Simulation[/bold cyan]

[green]Runtime:[/green] {time_str}
[green]Total Sites:[/green] {self.config.number_of_sites:,}
[green]Total Events:[/green] {self.total_events:,}
[green]Avg Throughput:[/green] {avg_throughput:.6f} events/sec
[{throughput_color}]Recent Rate:[/{throughput_color}] {recent_throughput:.6f} events/sec
[blue]Target Rate:[/blue] 0.500000 events/sec
[blue]Next Event Est:[/blue] {next_event_str}
[red]Errors:[/red] {self.error_count}
        """.strip()

		return Panel(overview_text, title="📊 Overview", border_style="cyan")

	def create_sites_status_panel(self):
		"""Create site status summary panel (no individual site listing)"""
		total_sites = self.config.number_of_sites

		status_text = []
		status_text.append("[bold yellow]📊 Site Status Distribution[/bold yellow]\n")

		for status, count in self.site_status_counts.items():
			percentage = (count / total_sites * 100) if total_sites > 0 else 0

			# Color coding for different statuses
			color_map = {"Active": "green", "Burst": "bright_yellow", "Quiet": "red", "Starting": "blue"}
			color = color_map.get(status, "white")

			# Create progress bar (scaled for visibility)
			bar_length = max(1, int(percentage / 2)) if percentage > 0 else 0
			bar = "█" * bar_length + "░" * max(0, 50 - bar_length)

			status_text.append(f"[{color}]{status:8}[/{color}] {bar} {count:6,} ({percentage:5.1f}%)")

		# Add activity distribution summary (top 5 most common activity levels)
		if self.activity_distribution:
			status_text.append("\n[bold yellow]🎯 Top Activity Levels[/bold yellow]")
			sorted_activities = sorted(self.activity_distribution.items(), key=lambda x: x[1], reverse=True)
			for activity, count in sorted_activities[:5]:
				status_text.append(f"  [cyan]{activity:6}[/cyan] : {count:5,} sites")

		return Panel("\n".join(status_text), title="🌐 Site Status Summary", border_style="blue")

	def create_app_stats_panel(self):
		"""Create app usage statistics"""
		if not self.app_stats:
			return Panel("No events yet...", title="📱 App Usage")

		total = sum(self.app_stats.values())
		stats_text = []

		# Sort apps by usage
		sorted_apps = sorted(self.app_stats.items(), key=lambda x: x[1], reverse=True)

		for app, count in sorted_apps:
			percentage = (count / total * 100) if total > 0 else 0
			bar_length = int(percentage / 5)  # Scale bar
			bar = "█" * bar_length + "░" * (20 - bar_length)
			stats_text.append(f"{app:10} {bar} {count:5d} ({percentage:4.1f}%)")

		return Panel("\n".join(stats_text), title="📱 App Usage", border_style="yellow")

	def create_throughput_graph(self):
		"""Create throughput history graph optimized for low-frequency events"""
		if len(self.throughput_history) < 2:
			return Panel("Collecting data...\n(Updates every 30 seconds)", title="📈 Throughput History")

		graph_lines = []
		graph_lines.append("Recent Throughput (events/sec, 5-min windows):")
		graph_lines.append("")

		# Show last 15 data points
		recent_data = list(self.throughput_history)[-15:]

		if not recent_data:
			graph_lines.append("No data available")
		else:
			# Use target rate as scale reference
			target_rate = 0.5
			display_max = max(max(recent_data), target_rate) * 1.2  # 20% headroom

			for i, value in enumerate(recent_data):
				# Scale bar to show relative values
				bar_height = int((value / display_max) * 25) if display_max > 0 else 0
				bar = "█" * bar_height + "░" * (25 - bar_height)

				# Show time offset (30-second intervals)
				time_offset = (len(recent_data) - i) * 30
				minutes_ago = time_offset // 60

				# Color code based on proximity to target
				if abs(value - target_rate) < 0.05:
					color = "green"
				elif abs(value - target_rate) < 0.1:
					color = "yellow"
				else:
					color = "red"

				graph_lines.append(f"{minutes_ago:2d}m ago: [{color}]{bar}[/{color}] {value:.6f}")

			graph_lines.append("")
			graph_lines.append(f"Scale: 0 to {display_max:.4f} events/sec")
			graph_lines.append(f"[green]Target: {target_rate:.6f} events/sec[/green]")

		return Panel("\n".join(graph_lines), title="📈 Throughput History", border_style="green")

	def create_layout(self):
		"""Create the complete dashboard layout optimized for large scale simulation"""
		# Create all layout components
		overview_panel = self.create_overview_panel()
		app_stats_panel = self.create_app_stats_panel()
		throughput_panel = self.create_throughput_graph()
		sites_status_panel = self.create_sites_status_panel()

		# Create layout structure
		layout = Layout()

		layout.split_column(
			Layout(overview_panel, size=11),
			Layout(name="middle", ratio=2),
			Layout(sites_status_panel, name="bottom", size=16),
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
		console.print("[bold green]🚀 Initializing Large Scale Simulation...[/bold green]")
		console.print(f"[blue]Creating {self.config.number_of_sites:,} site simulators...[/blue]")

		# Initialize sites with dashboard callback
		self.sites = []
		batch_size = 1000

		# Create sites in batches to avoid memory issues
		for batch_start in range(0, self.config.number_of_sites, batch_size):
			batch_end = min(batch_start + batch_size, self.config.number_of_sites)
			batch_sites = [
				SiteSimulator(i, self.config, self.dashboard.update_stats)
				for i in range(batch_start + 1, batch_end + 1)
			]
			self.sites.extend(batch_sites)
			console.print(f"[green]Created sites {batch_start + 1:,} to {batch_end:,}[/green]")

		console.print(f"[green]✅ All {len(self.sites):,} sites created![/green]")
		console.print("[blue]Starting simulation with live dashboard...[/blue]")
		console.print("[dim]Note: Events will be very infrequent (~0.5/sec total). Be patient![/dim]")

		self.is_running = True

		# Start simulation with live dashboard
		with Live(self.dashboard.create_layout(), refresh_per_second=0.1, screen=True) as live:
			try:

				async def update_dashboard():
					while self.is_running:
						try:
							live.update(self.dashboard.create_layout())
						except Exception:
							pass  # Silently handle dashboard errors
						await asyncio.sleep(10.0)  # Update every 10 seconds

				# Create all site simulation tasks
				site_tasks = [site.simulate() for site in self.sites]

				# Run dashboard updater and all site simulations
				await asyncio.gather(update_dashboard(), *site_tasks, return_exceptions=True)

			except KeyboardInterrupt:
				console.print("\n[red]⏹️  Simulation interrupted by user[/red]")
			finally:
				self.is_running = False

				# Mark all sites as inactive
				for site in self.sites:
					site.is_active = False

				# Final summary
				elapsed = time.time() - self.dashboard.start_time
				console.print("\n" + "=" * 70)
				console.print("[bold green]📊 Final Summary[/bold green]")
				console.print(
					f"[cyan]Runtime:[/cyan] {elapsed // 3600:.0f}h {(elapsed % 3600) // 60:.0f}m {elapsed % 60:.0f}s"
				)
				console.print(f"[cyan]Total Events:[/cyan] {self.dashboard.total_events:,}")
				console.print(
					f"[cyan]Average Throughput:[/cyan] {self.dashboard.total_events / elapsed:.6f} events/sec"
				)
				console.print("[cyan]Target Throughput:[/cyan] 0.500000 events/sec")
				console.print(
					f"[cyan]Accuracy:[/cyan] {abs(self.dashboard.total_events / elapsed - 0.5):.6f} deviation"
				)
				if self.dashboard.error_count > 0:
					console.print(f"[red]Errors:[/red] {self.dashboard.error_count}")
				console.print("=" * 70)


@click.command()
@click.option("--sites", default=5000, help="Number of sites to simulate")
@click.option("--min-interval", default=8000, help="Minimum event interval (seconds)")
@click.option("--max-interval", default=12000, help="Maximum event interval (seconds)")
@click.option("--apps", default="frappe,erpnext,gameplan,hrms,helpdesk", help="Comma-separated list of apps")
@click.option("--target-rate", default=0.5, help="Target events per second across all sites")
def main(sites, min_interval, max_interval, apps, target_rate):
	"""🚀 Large Scale Telemetry Simulation with Live Dashboard

	Simulates thousands of sites sending telemetry events at realistic intervals.
	Optimized for very low frequency events (~0.5 events/sec total).

	Press Ctrl+C to stop the simulation at any time.
	"""
	console.print("[bold green]🚀 Starting Large Scale Telemetry Simulation...[/bold green]")

	# Update config based on CLI args
	config = SimulationConfig()
	config.number_of_sites = sites
	config.min_event_interval = min_interval
	config.max_event_interval = max_interval
	config.apps = [app.strip() for app in apps.split(",")]

	# Auto-adjust intervals if target rate is specified
	if target_rate != 0.5:
		events_per_site_per_sec = target_rate / sites
		avg_interval = 1.0 / events_per_site_per_sec
		config.min_event_interval = int(avg_interval * 0.8)
		config.max_event_interval = int(avg_interval * 1.2)
		console.print(f"[yellow]Auto-adjusted intervals for {target_rate} events/sec target[/yellow]")

	console.print(f"[cyan]Configuration:[/cyan] {sites:,} sites, {len(config.apps)} apps")
	console.print(f"[cyan]Intervals:[/cyan] {config.min_event_interval}-{config.max_event_interval}s")
	console.print(f"[cyan]Target Rate:[/cyan] {target_rate} events/sec")
	console.print("[dim]Starting in 3 seconds... Press Ctrl+C to stop anytime[/dim]")
	time.sleep(3)

	simulation = TelemetrySimulation(config)

	try:
		asyncio.run(simulation.run_simulation())
	except KeyboardInterrupt:
		console.print("\n[yellow]Shutting down gracefully...[/yellow]")


if __name__ == "__main__":
	main()
