# pgperf - Postgres Performance Tests
This project is a scratchpad for me to test out the performance of a couple
of operations in Postgres.

## Operation 1 - Bulk Upsert - Pre-9.5 Version
Insert _n_ rows into a table, where some are new and others are updates.
Prior to Postgres 9.5, there was no way to perform a merge of a record in
one statement.

## Operation 2 - Bulk Upsert - 9.5+ Version
Same as Operation 1, but using the new `ON CONFLICT` feature.
