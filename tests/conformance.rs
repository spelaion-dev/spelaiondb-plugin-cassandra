//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042): it starts, speaks the protocol's version, opens sessions, returns results in the
//! order the contract says, fails without hanging and ends when its input closes. CQL has no
//! statement that only waits, so the set's cancel check is skipped; `tests/cassandra.rs` cancels
//! a long read instead.
//!
//! It needs the test servers (`ci/services.sh`, `SPELAION_TEST_CASSANDRA=127.0.0.1:19042` and
//! `SPELAION_TEST_SCYLLA=127.0.0.1:19043`). Without the variables it is skipped, which does not
//! count as tested.

use spelaion_driver_api::{ConnectParams, Secret};
use spelaion_plugin_host::conformance::{Target, assert_conforms};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-cassandra");

fn target(addr: &str, login: bool) -> Target {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let mut params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        ..Default::default()
    };
    if login {
        params.user = "cassandra".into();
        params.password = Some(Secret::new("cassandra"));
    }
    Target {
        program: PROGRAM.into(),
        engine: "cassandra".into(),
        params,
        one_row: "SELECT release_version FROM system.local".into(),
        refused: "SELECT * FROM system.a_table_that_is_not_there".into(),
        slow: None,
    }
}

#[tokio::test]
async fn the_plugin_conforms_on_cassandra() {
    let Ok(addr) = std::env::var("SPELAION_TEST_CASSANDRA") else {
        eprintln!("SPELAION_TEST_CASSANDRA not set: test skipped");
        return;
    };
    assert_conforms(&target(&addr, true)).await;
}

#[tokio::test]
async fn the_plugin_conforms_on_scylladb() {
    let Ok(addr) = std::env::var("SPELAION_TEST_SCYLLA") else {
        eprintln!("SPELAION_TEST_SCYLLA not set: test skipped");
        return;
    };
    assert_conforms(&target(&addr, false)).await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself. The program says it without a server.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
