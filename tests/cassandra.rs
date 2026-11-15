//! Cassandra through this plugin's own program, as SpelaionDB's core reaches it: every CQL type
//! as a cell (the type zoo), results read a page at a time, keyspaces and tables in the explorer,
//! errors, cancel, and what connecting refuses.
//!
//! It needs the test servers (`ci/services.sh`): `SPELAION_TEST_CASSANDRA=127.0.0.1:19042`, and
//! `SPELAION_TEST_SCYLLA=127.0.0.1:19043` for the tests that also run on ScyllaDB. Without them
//! it is skipped, which does not count as tested.

use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
};
use futures_util::StreamExt;
use spelaion_driver_api::{
    ColumnMeta, ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions,
    LogicalType, ObjectKind, Secret,
};
use spelaion_plugin_host::Plugin;

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-cassandra");

/// Each server set, with whether it asks for a login.
fn servers() -> Vec<(&'static str, String, bool)> {
    [
        ("Cassandra", "SPELAION_TEST_CASSANDRA", true),
        ("ScyllaDB", "SPELAION_TEST_SCYLLA", false),
    ]
    .into_iter()
    .filter_map(|(name, variable, login)| match std::env::var(variable) {
        Ok(addr) => Some((name, addr, login)),
        Err(_) => {
            eprintln!("{variable} not set: skipped");
            None
        }
    })
    .collect()
}

fn params(addr: &str, login: bool, keyspace: &str) -> ConnectParams {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let mut params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        database: keyspace.into(),
        connect_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    };
    if login {
        params.user = "cassandra".into();
        params.password = Some(Secret::new("cassandra"));
    }
    params
}

async fn connect(plugin: &Plugin, addr: &str, login: bool, keyspace: &str) -> Box<dyn Connection> {
    tokio::time::timeout(
        Duration::from_secs(60),
        plugin
            .driver("cassandra")
            .connect(&params(addr, login, keyspace)),
    )
    .await
    .expect("connecting in time")
    .expect("connecting")
}

struct Outcome {
    columns: Vec<ColumnMeta>,
    batches: Vec<arrow::array::RecordBatch>,
}

async fn run(c: &dyn Connection, cql: &str, batch: usize) -> Vec<Result<ExecEvent, DriverError>> {
    let options = ExecOptions {
        batch_size: batch,
        ..Default::default()
    };
    match c.execute(cql, &options).await {
        Ok(events) => tokio::time::timeout(Duration::from_secs(120), events.collect())
            .await
            .expect("the statement ended in time"),
        Err(error) => vec![Err(error)],
    }
}

async fn ok(c: &dyn Connection, cql: &str) -> Outcome {
    ok_in(c, cql, 1000).await
}

async fn ok_in(c: &dyn Connection, cql: &str, batch: usize) -> Outcome {
    let mut columns = Vec::new();
    let mut batches = Vec::new();
    let mut done = false;
    for event in run(c, cql, batch).await {
        match event {
            Ok(ExecEvent::Columns(schema)) => columns = schema.columns.clone(),
            Ok(ExecEvent::Batch(batch)) => batches.push(batch),
            Ok(ExecEvent::Notice(_)) => {}
            Ok(ExecEvent::Done { .. }) => done = true,
            Err(error) => panic!("{cql} failed: {error}"),
        }
    }
    assert!(done, "{cql} never ended");
    Outcome { columns, batches }
}

impl Outcome {
    fn column(&self, name: &str) -> (usize, &ColumnMeta) {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
            .unwrap_or_else(|| panic!("no column {name}"))
    }

    fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    fn array<T: 'static>(&self, name: &str) -> &T {
        let (i, _) = self.column(name);
        self.batches[0]
            .column(i)
            .as_any()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("{name} is not the array expected"))
    }
}

async fn fresh_keyspace(c: &dyn Connection, keyspace: &str) {
    ok(c, &format!("DROP KEYSPACE IF EXISTS {keyspace}")).await;
    ok(
        c,
        &format!(
            "CREATE KEYSPACE {keyspace} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ),
    )
    .await;
}

#[tokio::test]
async fn every_cql_type_is_a_cell() {
    for (name, addr, login) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, login, "").await;
        fresh_keyspace(&*c, "spelaion_zoo").await;
        c.select_database("spelaion_zoo").await.unwrap();
        ok(&*c, "CREATE TYPE address (street text, number int)").await;
        ok(
            &*c,
            "CREATE TABLE zoo (id int PRIMARY KEY, t tinyint, s smallint, b bigint, \
             f float, d double, bo boolean, tx text, a ascii, bl blob, ts timestamp, \
             dt date, tm time, u uuid, tu timeuuid, de decimal, vi varint, ip inet, \
             du duration, li list<int>, se set<text>, ma map<text, int>, tp tuple<int, text>, \
             ad frozen<address>)",
        )
        .await;
        ok(
            &*c,
            "INSERT INTO zoo (id, t, s, b, f, d, bo, tx, a, bl, ts, dt, tm, u, tu, de, vi, ip, \
             du, li, se, ma, tp, ad) VALUES (1, 7, 300, 9223372036854775807, 1.5, 2.25, true, \
             'ñandú', 'abc', 0xcafe, '2024-02-29 12:34:56.789+0000', '2024-02-29', \
             '12:34:56.123456', 123e4567-e89b-12d3-a456-426614174000, \
             d2177dd0-eaa2-11de-a572-001b779c76e3, 12345678901234567890.123456789, \
             123456789012345678901234567890, '10.0.0.1', 1h30m, [1, 2], {'x', 'y'}, \
             {'k': 1}, (3, 'z'), {street: 'Main', number: 5})",
        )
        .await;
        let row = ok(&*c, "SELECT * FROM zoo WHERE id = 1").await;
        let logical = |column: &str| row.column(column).1.logical;
        let native = |column: &str| row.column(column).1.native_type.clone();
        for (column, expected) in [
            ("t", LogicalType::Int),
            ("s", LogicalType::Int),
            ("b", LogicalType::Int),
            ("f", LogicalType::Float),
            ("bo", LogicalType::Bool),
            ("tx", LogicalType::Text),
            ("bl", LogicalType::Bytes),
            ("ts", LogicalType::TimestampTz),
            ("dt", LogicalType::Date),
            ("tm", LogicalType::Time),
            ("u", LogicalType::Uuid),
            ("de", LogicalType::Decimal),
            ("du", LogicalType::Interval),
            ("li", LogicalType::Json),
            ("ad", LogicalType::Json),
        ] {
            assert_eq!(logical(column), expected, "{name}: {column}");
        }
        assert_eq!(native("ma"), "map<text, int>", "{name}");
        assert_eq!(native("ad"), "spelaion_zoo.address", "{name}");
        assert_eq!(row.array::<Int64Array>("b").value(0), i64::MAX, "{name}");
        assert_eq!(row.array::<Float64Array>("f").value(0), 1.5, "{name}");
        assert!(row.array::<BooleanArray>("bo").value(0), "{name}");
        assert_eq!(row.array::<StringArray>("tx").value(0), "ñandú", "{name}");
        assert_eq!(
            row.array::<BinaryArray>("bl").value(0),
            [0xca, 0xfe],
            "{name}"
        );
        // 2024-02-29 12:34:56.789 UTC, in microseconds.
        assert_eq!(
            row.array::<TimestampMicrosecondArray>("ts").value(0),
            1_709_210_096_789_000,
            "{name}"
        );
        assert_eq!(row.array::<Date32Array>("dt").value(0), 19_782, "{name}");
        assert_eq!(
            row.array::<Time64MicrosecondArray>("tm").value(0),
            45_296_123_456,
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("u").value(0),
            "123e4567-e89b-12d3-a456-426614174000",
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("de").value(0),
            "12345678901234567890.123456789",
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("vi").value(0),
            "123456789012345678901234567890",
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("ip").value(0),
            "10.0.0.1",
            "{name}"
        );
        assert_eq!(row.array::<StringArray>("li").value(0), "[1,2]", "{name}");
        assert_eq!(
            row.array::<StringArray>("se").value(0),
            r#"["x","y"]"#,
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("ma").value(0),
            r#"{"k":1}"#,
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("tp").value(0),
            r#"[3,"z"]"#,
            "{name}"
        );
        assert_eq!(
            row.array::<StringArray>("ad").value(0),
            r#"{"street":"Main","number":5}"#,
            "{name}"
        );
        // A row of nulls.
        ok(&*c, "INSERT INTO zoo (id) VALUES (2)").await;
        let nulls = ok(&*c, "SELECT tx, b, ts FROM zoo WHERE id = 2").await;
        assert!(nulls.batches[0].column(0).is_null(0), "{name}");
        assert!(nulls.batches[0].column(1).is_null(0), "{name}");
    }
}

#[tokio::test]
async fn a_result_is_read_a_page_at_a_time() {
    for (name, addr, login) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, login, "").await;
        fresh_keyspace(&*c, "spelaion_pages").await;
        c.select_database("spelaion_pages").await.unwrap();
        ok(&*c, "CREATE TABLE t (p int, n int, PRIMARY KEY (p, n))").await;
        let mut batch = String::from("BEGIN UNLOGGED BATCH ");
        for n in 0..250 {
            batch.push_str(&format!("INSERT INTO t (p, n) VALUES (1, {n}); "));
        }
        batch.push_str("APPLY BATCH");
        ok(&*c, &batch).await;
        let pages = ok_in(&*c, "SELECT n FROM t WHERE p = 1", 100).await;
        assert_eq!(pages.rows(), 250, "{name}");
        assert_eq!(pages.batches.len(), 3, "{name}: pages of 100");
        // An empty result still has its columns.
        let empty = ok(&*c, "SELECT n FROM t WHERE p = 2").await;
        assert_eq!(empty.columns.len(), 1, "{name}");
        assert_eq!(empty.rows(), 0, "{name}");
    }
}

#[tokio::test]
async fn keyspaces_and_tables_are_what_the_explorer_shows() {
    for (name, addr, login) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, login, "").await;
        fresh_keyspace(&*c, "spelaion_explorer").await;
        c.select_database("spelaion_explorer").await.unwrap();
        assert_eq!(
            c.current_database().await.unwrap().as_deref(),
            Some("spelaion_explorer"),
            "{name}"
        );
        ok(
            &*c,
            "CREATE TABLE orders (customer int, placed timestamp, id uuid, total decimal, \
             note text, PRIMARY KEY ((customer), placed, id)) WITH comment = 'Orders by customer'",
        )
        .await;
        ok(&*c, "CREATE INDEX orders_note ON orders (note)").await;
        let schemas = c.list_schemas().await.unwrap();
        let ours = schemas
            .iter()
            .find(|s| s.name == "spelaion_explorer")
            .expect("listed");
        assert!(!ours.system, "{name}");
        assert!(
            schemas.iter().any(|s| s.name == "system" && s.system),
            "{name}"
        );
        let objects = c.list_objects("spelaion_explorer").await.unwrap();
        assert_eq!(objects.len(), 1, "{name}: {objects:?}");
        assert_eq!(objects[0].kind, ObjectKind::Table, "{name}");
        assert_eq!(
            objects[0].comment.as_deref(),
            Some("Orders by customer"),
            "{name}"
        );
        let table = c
            .describe_table("spelaion_explorer", "orders")
            .await
            .unwrap();
        assert_eq!(table.primary_key, ["customer", "placed", "id"], "{name}");
        let names: Vec<_> = table.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["customer", "placed", "id", "note", "total"],
            "{name}"
        );
        assert_eq!(table.columns[4].native_type, "decimal", "{name}");
        assert!(
            !table.columns[0].nullable && table.columns[4].nullable,
            "{name}"
        );
        assert_eq!(table.indexes.len(), 1, "{name}");
        assert_eq!(table.indexes[0].name, "orders_note", "{name}");
        let columns = c.list_columns("spelaion_explorer").await.unwrap();
        assert_eq!(columns.len(), 5, "{name}");
        // Choosing a keyspace runs what follows there; a missing one is an error.
        assert!(
            c.select_database("no_such_keyspace").await.is_err(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_server_error_and_a_cancel_leave_the_session_working() {
    for (name, addr, login) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, login, "").await;
        let refused = run(&*c, "SELECT * FROM system.nope", 100).await;
        assert!(
            refused
                .iter()
                .any(|e| matches!(e, Err(DriverError::Database { .. }))),
            "{name}: {refused:?}"
        );
        // A cancel while the pages of a long read are taken one by one.
        let options = ExecOptions {
            batch_size: 1,
            ..Default::default()
        };
        let mut events = c
            .execute("SELECT * FROM system_schema.columns", &options)
            .await
            .unwrap();
        let _columns = events.next().await;
        let started = Instant::now();
        c.cancel_handle().cancel().await.unwrap();
        let rest: Vec<_> = tokio::time::timeout(Duration::from_secs(10), events.collect())
            .await
            .expect("the cancel was obeyed in time");
        assert!(
            rest.iter()
                .any(|e| matches!(e, Err(DriverError::Cancelled))),
            "{name}: {} events after the cancel",
            rest.len()
        );
        assert!(started.elapsed() < Duration::from_secs(5), "{name}");
        assert_eq!(
            ok(&*c, "SELECT release_version FROM system.local")
                .await
                .rows(),
            1,
            "{name}"
        );
    }
}

#[tokio::test]
async fn connecting_refuses_what_it_cannot_honour() {
    let Ok(addr) = std::env::var("SPELAION_TEST_CASSANDRA") else {
        eprintln!("SPELAION_TEST_CASSANDRA not set: skipped");
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let driver = plugin.driver("cassandra");
    let mut wrong = params(&addr, true, "");
    wrong.password = Some(Secret::new("not-the-password"));
    let error = driver.connect(&wrong).await.err().expect("refused");
    let text = error.to_string();
    assert!(!text.contains("not-the-password"), "{text}");
    let mut read_only = params(&addr, true, "");
    read_only.read_only = true;
    assert!(driver.connect(&read_only).await.is_err());
    let mut options = params(&addr, true, "");
    options.options.insert("consistency".into(), "ALL".into());
    assert!(driver.connect(&options).await.is_err());
}
