STREAM_NAME = "pulse:events"

LOGGER_NAME = "pulse"

STREAM_MAX_LENGTH = 100_000


# Pending recovery
# Minimum idle time (in milliseconds) before we try to steal/claim a pending
# message from another consumer. Keep small enough to recover quickly after
# a crash, but large enough to not interfere with actively processing workers.
PENDING_MIN_IDLE_MS = 5000


# Event shape
# The ingest key is public by design, so these limits — not authentication — are
# what keep the event table usable. See pulse/validation.py.

# Lowercase snake_case, optionally namespaced with ':' (e.g. `helpdesk:ticket_created`).
# Anything else is a bug at the call site: a name built from translated UI copy, a
# doctype concatenated onto a literal, or an injection probe against the endpoint.
EVENT_NAME_PATTERN = r"^[a-z][a-z0-9_:]{1,63}$"

# The client's idempotency key: machine-generated, so anything else is a bug.
EVENT_ID_PATTERN = r"^[A-Za-z0-9_-]{8,64}$"

# A property bag describes an event; it is not a place to ship a document.
MAX_PROPERTIES_LENGTH = 4096
MAX_PROPERTY_VALUE_LENGTH = 512
MAX_PROPERTY_DEPTH = 5
MAX_PROPERTY_ITEMS = 50

# How far a client clock may run ahead before we stop believing it, and how old an
# event may be and still be accepted.
MAX_CLOCK_SKEW_MINUTES = 5
MAX_EVENT_AGE_DAYS = 30
