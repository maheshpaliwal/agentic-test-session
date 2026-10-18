#!/bin/sh
# Dedicated role + database for the tsession collector (separate from any app DB).
set -eu
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d postgres <<SQL
CREATE ROLE tsession LOGIN PASSWORD '${TSESSION_DB_PASSWORD}';
CREATE DATABASE tsession OWNER tsession;
SQL
