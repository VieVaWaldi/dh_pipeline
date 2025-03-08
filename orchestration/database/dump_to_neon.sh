#!/bin/bash

#SBATCH --job-name=dump_to_neon
#SBATCH --partition=standard
#SBATCH --time=4:00:00
#SBATCH --output=/home/lu72hip/DIGICHer/DIGICHer_Pipeline/data/runs/database/dump_to_neon_%j.log

### CleanUp

cleanup() {
   echo "Cleaning up..."
   pg_ctl stop -D "$PGDATA"
   exit
}
trap cleanup EXIT SIGINT SIGTERM

### Setup

echo "${PGDATA:?not set}"
pg_ctl -D $PGDATA stop # Stop it in case its running on the login node
pg_ctl -D $PGDATA start -l /vast/lu72hip/pgsql/logfile
sleep 5
echo "PostgreSQL status:"
pg_ctl status -D $PGDATA

### Run

cd /vast/lu72hip/pgsql_dumps

# pg_dump --table topics --table projects_topics db_digicher | psql "postgresql://db_digicher_owner:qBa0z4HOLWPA@ep-old-meadow-a20eql9r-pooler.eu-central-1.aws.neon.tech/db_digicher?sslmode=require"

#pg_dump -F p --column-inserts --data-only --table="weblinks" --where-condition="id IN (SELECT weblink_id FROM projects_weblinks)" dbname > project_weblinks.sql