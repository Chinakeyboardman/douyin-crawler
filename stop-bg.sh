#!/usr/bin/env bash
exec "$(dirname "$0")/scripts/stop-celery.sh" "$@"
