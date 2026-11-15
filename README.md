# spelaiondb-plugin-cassandra

Apache Cassandra for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), served as a
plugin: a program SpelaionDB starts and talks to over its standard input and output (its
ADR-041). It also serves ScyllaDB, which speaks Cassandra's protocol.

SpelaionDB's core splits and guards CQL with lists of its own (its ADR-056) and sends each
statement as it is written; the plugin runs it and returns its rows a page at a time. The
explorer shows keyspaces, tables and materialized views, with their columns, primary key and
indexes. Read-only mode and agents are not offered on Cassandra yet.

- **Tested against:** Apache Cassandra 5 and ScyllaDB 6.2 (`testing/compose.yaml`).
- **Not tested here:** AWS Keyspaces and DataStax Astra (paid services with no local image).
  The plugin connects only to the address SpelaionDB gives it: every node a cluster names is
  reached through that address.

Protocol: [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
version 1.1, experimental until SpelaionDB 1.0. Rules for contributors and agents: `AGENTS.md`.
License: Apache-2.0.
