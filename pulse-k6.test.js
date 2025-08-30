/* global __ENV */
import { randomSeed } from 'k6';
import { check, sleep } from 'k6';
import http from 'k6/http';

randomSeed(12345);

export let options = {
  scenarios: {
    // Variable rate telemetry - mix of low and high activity
    variable_telemetry: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 10,
      maxVUs: 50,
      stages: [
        { duration: '1m', target: 2 },   // Ramp up slowly
        { duration: '2m', target: 4 },   // High activity period
        { duration: '30s', target: 2 },   // Medium activity
        { duration: '10s', target: 6 },  // Peak burst
        // { duration: '2m', target: 1 },   // Quiet period
        // { duration: '1m', target: 3 },   // Another active period
        // { duration: '2m', target: 2 },   // Wind down
        // Pattern repeats for longer durations
      ]
    }
  },
  thresholds: {
    'http_req_duration': ['p(95)<3000', 'p(99)<5000'],
    'http_req_failed': ['rate<0.01'], // Less than 1% failures
    'checks': ['rate>0.99'], // 99%+ success rate
  }
};

const FRAPPE_APPS = [
  'frappe', 'erpnext', 'hrms', 'helpdesk', 'crm', 'lms', 'ecommerce',
  'builder', 'insights', 'wiki', 'gameplan', 'raven', 'print_designer'
];

const SITE_DOMAINS = [
  'com', 'org', 'net', 'io', 'co', 'app', 'dev', 'tech', 'biz'
];

function generateSiteId() {
  const prefix = ['demo', 'test', 'prod', 'staging', 'dev', 'app'];
  const suffix = ['corp', 'inc', 'solutions', 'systems', 'tech', 'group'];
  const domain = SITE_DOMAINS[Math.floor(Math.random() * SITE_DOMAINS.length)];

  return `${prefix[Math.floor(Math.random() * prefix.length)]}-${suffix[Math.floor(Math.random() * suffix.length)]}-${Math.floor(Math.random() * 9999)}.${domain}`;
}

function generateAppVersion() {
  return `${Math.floor(Math.random() * 5) + 13}.${Math.floor(Math.random() * 20)}.${Math.floor(Math.random() * 10)}`;
}

function makeEvent() {
  const siteId = generateSiteId();
  const app = FRAPPE_APPS[Math.floor(Math.random() * FRAPPE_APPS.length)];

  // Simulate apps being active at different times within the 6-hour window
  const hoursAgo = Math.floor(Math.random() * 6);
  const minutesAgo = Math.floor(Math.random() * 60);
  const lastActive = new Date(Date.now() - (hoursAgo * 60 + minutesAgo) * 60 * 1000);

  return {
    event_name: 'app.active',
    site: siteId,
    app: app,
    captured_at: new Date().toISOString(),
    properties: {
      last_active_at: lastActive.toISOString(),
      app_version: generateAppVersion(),
      site_plan: ['free', 'starter', 'business', 'enterprise'][Math.floor(Math.random() * 4)],
      active_users_6h: Math.floor(Math.random() * 50) + 1,
      requests_6h: Math.floor(Math.random() * 1000) + 10
    }
  };
}

const API_TOKEN = 'OAN1ZcaC8vsgslu7QXQrUPDTuUgk4Pxl'


export default function () {
  const url = 'https://pulse.m.frappe.cloud/api/method/pulse.api.ingest';

  const event = makeEvent();
  const payload = JSON.stringify(event);

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Pulse-API-Key': API_TOKEN
    },
    timeout: '30s'
  };

  let res = http.post(url, payload, params);

  check(res, {
    'status is 2xx': (r) => r.status >= 200 && r.status < 300,
    'response time < 3s': (r) => r.timings.duration < 3000,
    'no server errors': (r) => r.status < 500
  });

  // Small random delay
  sleep(Math.random() * 0.3);
}
