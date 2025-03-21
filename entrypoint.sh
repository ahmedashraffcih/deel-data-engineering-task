#!/bin/sh
set -e

# If a command is provided, execute it
if [ "$#" -gt 0 ]; then
    exec python -m src.cli "$@"
fi

# Default behavior: Keep the container running
echo "Container started. You can run commands inside using: docker exec -it <container_id> python -m src.cli <command>"
tail -f /dev/null
