#!/bin/bash

# Configuration
PGDATA="/vast/lu72hip/pgsql/data"
PGPORT=5433
LOGFILE="$PGDATA/postgres_server.log"

# Check if postgres is already running on port 5433
check_postgres() {
    netstat -an | grep LISTEN | grep ":${PGPORT}" > /dev/null
    return $?
}

# Ensure data directory exists
if [ ! -d "$PGDATA" ]; then
    echo "Error: Database directory $PGDATA does not exist"
    echo "Please run: initdb -D $PGDATA"
    exit 1
fi

# Check if already running
if check_postgres; then
    echo "PostgreSQL is already running on port ${PGPORT}"
    exit 0
fi

# Check if postgresql.conf has correct port
if ! grep -q "^port = ${PGPORT}" "$PGDATA/postgresql.conf"; then
    echo "Setting port to ${PGPORT} in postgresql.conf"
    sed -i "s/^#\?port = .*/port = ${PGPORT}/" "$PGDATA/postgresql.conf"
fi

# Start PostgreSQL
echo "Starting PostgreSQL on port ${PGPORT}..."
pg_ctl -D "$PGDATA" -l "$LOGFILE" start

# Wait a moment for server to start
sleep 2

# Verify it's running
if check_postgres; then
    echo "PostgreSQL successfully started on port ${PGPORT}"
    echo "Log file: $LOGFILE"
else
    echo "Failed to start PostgreSQL. Check the log file: $LOGFILE"
    exit 1
fi