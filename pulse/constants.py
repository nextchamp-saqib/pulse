LOGGER_NAME = "pulse"

# API limits
API_RATE_LIMIT = 100
API_RATE_LIMIT_SECONDS = 60

# Redis Streams / Consumer Groups
CONSUMER_GROUP_NAME = "event_processors"
STREAM_MAX_LENGTH = 100000
CONSUMER_GROUP_EXISTS_ERROR = "BUSYGROUP"

# Pending recovery
# Minimum idle time (in milliseconds) before we try to steal/claim a pending
# message from another consumer. Keep small enough to recover quickly after
# a crash, but large enough to not interfere with actively processing workers.
PENDING_MIN_IDLE_MS = 5000
