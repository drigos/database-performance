#!/bin/bash

shopt -s expand_aliases

alias bench="pgbench -h localhost -p 5432 -U postgres -c 10 -j 2 -T 300"
tables=(
    "serial"
    "uuid-v4-type-uuid"
    "uuid-v7-type-uuid"
    "ulid-type-uuid"
    "uuid-v4-type-char"
    "uuid-v7-type-char"
    "ulid-type-char"
    "ulid-type-base32"
)
for table_type in "${tables[@]}"; do
    inserted_records=$(
        bench -f "insert-id-${table_type}.sql" db_performance 2>/dev/null \
        | awk '/processed/ {print $NF}'
    )
    echo "Inserted: ${inserted_records} (${table_type})"

    selected_records=$(
        bench -f "select-id-${table_type}.sql" -D max="${inserted_records}" db_performance 2>/dev/null \
        | awk '/processed/ {print $NF}'
    )
    echo "Selected: ${selected_records} (${table_type})"
done
