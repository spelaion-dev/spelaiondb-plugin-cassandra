//! Cassandra's driver for SpelaionDB: Apache Cassandra and the servers that speak its protocol
//! (ScyllaDB), served as a plugin (SpelaionDB's ADR-041, ADR-056).
//!
//! It is sent statements of CQL, a language shaped like SQL that SpelaionDB's core splits and
//! guards with lists of its own; this driver runs each as it is written. Results are read a page
//! at a time, the next page fetched only when the core has taken the last one.
//!
//! The core opens every route to the server (tunnels, proxies, Kubernetes): this driver
//! connects only to the address it is given. The other nodes a cluster names are reached through
//! that same address, never at their own, which a tunnel could not reach.

mod values;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::response::query_result::IntoRowsResultError;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::Statement;
use scylla::value::Row;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ColumnInfo, ConnectParams, Connection, Driver, DriverError,
    EventStream, ExecEvent, ExecOptions, IndexInfo, KeepAlive, ObjectInfo, ObjectKind, Result,
    SchemaColumn, SchemaInfo, TableInfo,
};
use tokio::sync::{Notify, mpsc};

pub use values::{json, text};

/// The most rows one page asks the server for, whatever the core's batch size.
const MAX_PAGE: usize = 5000;

/// The `cassandra` engine.
#[derive(Debug, Default, Clone)]
pub struct CassandraDriver;

#[async_trait]
impl Driver for CassandraDriver {
    /// The name SpelaionDB's configuration uses: `engine = "cassandra"`.
    fn engine(&self) -> &str {
        "cassandra"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if params.read_only {
            // SpelaionDB's core does not open such a connection (ADR-056, point 6).
            return Err(DriverError::Connection(
                "Cassandra has no read-only mode in SpelaionDB yet".into(),
            ));
        }
        if let Some(key) = params.options.keys().next() {
            return Err(DriverError::Connection(format!(
                "Cassandra has no connection setting `{key}` here"
            )));
        }
        let address = match params.connect_addr {
            Some(addr) => addr,
            None => tokio::net::lookup_host((params.host.as_str(), params.port))
                .await
                .map_err(|e| {
                    DriverError::Connection(format!("{} cannot be found: {e}", params.host))
                })?
                .next()
                .ok_or_else(|| {
                    DriverError::Connection(format!("{} has no address", params.host))
                })?,
        };
        let mut builder = SessionBuilder::new()
            .known_node_addr(address)
            .address_translator(Arc::new(OnlyThrough(address)))
            .connection_timeout(params.connect_timeout())
            // The explorer reads `system_schema` itself, when it is asked.
            .fetch_schema_metadata(false);
        if !params.user.is_empty() {
            let password = params
                .password
                .as_ref()
                .map(|p| p.expose().to_string())
                .unwrap_or_default();
            builder = builder.user(params.user.clone(), password);
        }
        if let Some(config) = tls::config(params)? {
            builder = builder.tls_context(Some(Arc::new(config)));
        }
        if !params.database.is_empty() {
            builder = builder.use_keyspace(params.database.clone(), true);
        }
        let session = match tokio::time::timeout(params.connect_timeout(), builder.build()).await {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => return Err(connection_error(&error.to_string())),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    params.connect_timeout().as_secs()
                )));
            }
        };
        Ok(Box::new(CassandraConnection {
            session: Arc::new(session),
            capabilities: Capabilities {
                cancel: true,
                schemas: true,
                ..Capabilities::default()
            },
            running: Arc::new(Mutex::new(None)),
        }))
    }
}

/// Every node a cluster names is reached through the one address the plugin was given.
#[derive(Debug)]
struct OnlyThrough(SocketAddr);

#[async_trait]
impl AddressTranslator for OnlyThrough {
    async fn translate_address(
        &self,
        _peer: &UntranslatedPeer,
    ) -> std::result::Result<SocketAddr, TranslationError> {
        Ok(self.0)
    }
}

struct CassandraConnection {
    session: Arc<Session>,
    capabilities: Capabilities,
    /// Told when the statement that is running must stop.
    running: Arc<Mutex<Option<Arc<Notify>>>>,
}

#[async_trait]
impl Connection for CassandraConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        let cancelled = Arc::new(Notify::new());
        *self.running.lock().expect("lock poisoned") = Some(cancelled.clone());
        let session = self.session.clone();
        let mut statement = Statement::new(sql.to_string());
        let page = opts.batch_size.clamp(1, MAX_PAGE);
        statement.set_page_size(i32::try_from(page).unwrap_or(5000));
        // One event at a time: the core's credit decides when the next page is fetched.
        let (sender, receiver) = mpsc::channel::<Result<ExecEvent>>(1);
        tokio::spawn(async move {
            let run = async {
                let mut paging = PagingState::start();
                let mut schema = None;
                loop {
                    let (result, more) = session
                        .query_single_page(statement.clone(), &[], paging)
                        .await
                        .map_err(|e| database_error(&e.to_string()))?;
                    match result.into_rows_result() {
                        Ok(rows) => {
                            let schema = match &schema {
                                Some(schema) => schema,
                                None => {
                                    let columns: Vec<_> = rows
                                        .column_specs()
                                        .iter()
                                        .map(|spec| {
                                            let (logical, native) = values::column(spec.typ());
                                            (spec.name().to_string(), logical, native)
                                        })
                                        .collect();
                                    let made = values::schema(&columns);
                                    if sender
                                        .send(Ok(ExecEvent::Columns(made.clone())))
                                        .await
                                        .is_err()
                                    {
                                        return Ok(Stop::Gone);
                                    }
                                    schema.insert(made)
                                }
                            };
                            let page: Vec<Row> = rows
                                .rows::<Row>()
                                .map_err(|e| DriverError::Other(e.to_string()))?
                                .collect::<std::result::Result<_, _>>()
                                .map_err(|e| DriverError::Other(e.to_string()))?;
                            if !page.is_empty() {
                                let batch = values::batch(schema, &page)?;
                                if sender.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                                    return Ok(Stop::Gone);
                                }
                            }
                        }
                        // A write or a schema change: no rows.
                        Err(IntoRowsResultError::ResultNotRows(_)) => return Ok(Stop::Done),
                        Err(error) => return Err(DriverError::Other(error.to_string())),
                    }
                    match more {
                        PagingStateResponse::HasMorePages { state } => paging = state,
                        PagingStateResponse::NoMorePages => return Ok(Stop::Done),
                    }
                }
            };
            let outcome = tokio::select! {
                biased;
                () = cancelled.notified() => Err(DriverError::Cancelled),
                outcome = run => outcome,
            };
            let last = match outcome {
                Ok(Stop::Done) => Ok(ExecEvent::Done {
                    rows_affected: None,
                }),
                Ok(Stop::Gone) => return,
                Err(error) => Err(error),
            };
            let _ = sender.send(last).await;
        });
        Ok(Box::pin(stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
        )))
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(Cancel(self.running.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        let version = self
            .one_text("SELECT release_version FROM system.local", ())
            .await?;
        Ok(version.unwrap_or_default())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        self.session
            .query_unpaged("SELECT key FROM system.local", ())
            .await
            .map_err(|e| connection_error(&e.to_string()))?;
        Ok(KeepAlive::Alive)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(self.session.get_keyspace().map(|k| k.to_string()))
    }

    async fn database_context(&self) -> Result<Option<String>> {
        self.current_database().await
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        self.session
            .use_keyspace(database.to_string(), true)
            .await
            .map_err(|e| database_error(&e.to_string()))
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(self
            .list_schemas()
            .await?
            .into_iter()
            .map(|s| s.name)
            .collect())
    }

    /// A keyspace is what the explorer shows as a schema.
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let mut keyspaces: Vec<SchemaInfo> = self
            .texts("SELECT keyspace_name FROM system_schema.keyspaces", ())
            .await?
            .into_iter()
            .map(|name| SchemaInfo {
                system: name.starts_with("system") || name == "dse_system",
                name,
            })
            .collect();
        keyspaces.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(keyspaces)
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let mut objects = Vec::new();
        let tables = self
            .session
            .query_unpaged(
                "SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = ?",
                (schema,),
            )
            .await
            .map_err(|e| database_error(&e.to_string()))?
            .into_rows_result()
            .map_err(|e| DriverError::Other(e.to_string()))?;
        for row in tables
            .rows::<(String, Option<String>)>()
            .map_err(|e| DriverError::Other(e.to_string()))?
        {
            let (name, comment) = row.map_err(|e| DriverError::Other(e.to_string()))?;
            objects.push(ObjectInfo {
                schema: schema.to_string(),
                name,
                kind: ObjectKind::Table,
                // A count would read the table on every node.
                estimated_rows: None,
                comment: comment.filter(|c| !c.is_empty()),
            });
        }
        // ScyllaDB keeps each secondary index as a view named `<index>_index`: not the user's.
        let index_views: Vec<String> = self
            .texts(
                "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = ?",
                (schema,),
            )
            .await?
            .into_iter()
            .map(|index| format!("{index}_index"))
            .collect();
        for name in self
            .texts(
                "SELECT view_name FROM system_schema.views WHERE keyspace_name = ?",
                (schema,),
            )
            .await?
            .into_iter()
            .filter(|view| !index_views.contains(view))
        {
            objects.push(ObjectInfo {
                schema: schema.to_string(),
                name,
                kind: ObjectKind::MaterializedView,
                estimated_rows: None,
                comment: None,
            });
        }
        objects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(objects)
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let object = self
            .list_objects(schema)
            .await?
            .into_iter()
            .find(|o| o.name == name)
            .ok_or_else(|| DriverError::Other(format!("there is no table {schema}.{name}")))?;
        let rows = self
            .session
            .query_unpaged(
                "SELECT column_name, kind, position, type FROM system_schema.columns \
                 WHERE keyspace_name = ? AND table_name = ?",
                (schema, name),
            )
            .await
            .map_err(|e| database_error(&e.to_string()))?
            .into_rows_result()
            .map_err(|e| DriverError::Other(e.to_string()))?;
        let mut columns: Vec<(String, String, i32, String)> = Vec::new();
        for row in rows
            .rows::<(String, String, i32, String)>()
            .map_err(|e| DriverError::Other(e.to_string()))?
        {
            columns.push(row.map_err(|e| DriverError::Other(e.to_string()))?);
        }
        // The partition key, then the clustering columns, each by position; then the rest.
        let rank = |kind: &str| match kind {
            "partition_key" => 0,
            "clustering" => 1,
            "static" => 2,
            _ => 3,
        };
        columns.sort_by(|a, b| (rank(&a.1), a.2, &a.0).cmp(&(rank(&b.1), b.2, &b.0)));
        let primary_key = columns
            .iter()
            .filter(|c| rank(&c.1) < 2)
            .map(|c| c.0.clone())
            .collect();
        let mut indexes = Vec::new();
        let found = self
            .session
            .query_unpaged(
                "SELECT index_name, options FROM system_schema.indexes \
                 WHERE keyspace_name = ? AND table_name = ?",
                (schema, name),
            )
            .await
            .map_err(|e| database_error(&e.to_string()))?
            .into_rows_result()
            .map_err(|e| DriverError::Other(e.to_string()))?;
        for row in found
            .rows::<(String, std::collections::HashMap<String, String>)>()
            .map_err(|e| DriverError::Other(e.to_string()))?
        {
            let (index, options) = row.map_err(|e| DriverError::Other(e.to_string()))?;
            let target = options.get("target").cloned().unwrap_or_default();
            indexes.push(IndexInfo {
                definition: format!("CREATE INDEX {index} ON {schema}.{name} ({target})"),
                name: index,
                unique: false,
                primary: false,
            });
        }
        Ok(TableInfo {
            object,
            columns: columns
                .into_iter()
                .map(|(column, kind, _, native)| ColumnInfo {
                    name: column,
                    native_type: native,
                    nullable: rank(&kind) >= 2,
                    default: None,
                    comment: None,
                })
                .collect(),
            primary_key,
            indexes,
            foreign_keys: Vec::new(),
            constraints: Vec::new(),
        })
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        let rows = self
            .session
            .query_unpaged(
                "SELECT table_name, column_name, type FROM system_schema.columns \
                 WHERE keyspace_name = ?",
                (schema,),
            )
            .await
            .map_err(|e| database_error(&e.to_string()))?
            .into_rows_result()
            .map_err(|e| DriverError::Other(e.to_string()))?;
        // Only the tables and views the explorer lists (not ScyllaDB's index views).
        let listed: Vec<String> = self
            .list_objects(schema)
            .await?
            .into_iter()
            .map(|o| o.name)
            .collect();
        let mut columns = Vec::new();
        for row in rows
            .rows::<(String, String, String)>()
            .map_err(|e| DriverError::Other(e.to_string()))?
        {
            let (table, name, native_type) = row.map_err(|e| DriverError::Other(e.to_string()))?;
            if !listed.contains(&table) {
                continue;
            }
            columns.push(SchemaColumn {
                table,
                name,
                native_type,
            });
        }
        Ok(columns)
    }
}

impl CassandraConnection {
    /// The first column of every row of a catalog query, as text.
    async fn texts(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Vec<String>> {
        let rows = self
            .session
            .query_unpaged(cql, values)
            .await
            .map_err(|e| database_error(&e.to_string()))?
            .into_rows_result()
            .map_err(|e| DriverError::Other(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows
            .rows::<(String,)>()
            .map_err(|e| DriverError::Other(e.to_string()))?
        {
            out.push(row.map_err(|e| DriverError::Other(e.to_string()))?.0);
        }
        Ok(out)
    }

    async fn one_text(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Option<String>> {
        Ok(self.texts(cql, values).await?.into_iter().next())
    }
}

/// How a statement's run ended, short of an error.
enum Stop {
    Done,
    /// The core stopped reading: nobody is there to tell.
    Gone,
}

fn database_error(message: &str) -> DriverError {
    DriverError::Database {
        code: None,
        message: message.to_string(),
        detail: None,
        hint: None,
        position: None,
    }
}

fn connection_error(message: &str) -> DriverError {
    if message.contains("Authentication") || message.contains("authentication") {
        return DriverError::Connection(format!("the server refused the login: {message}"));
    }
    DriverError::Connection(message.to_string())
}

struct Cancel(Arc<Mutex<Option<Arc<Notify>>>>);

#[async_trait]
impl CancelHandle for Cancel {
    async fn cancel(&self) -> Result<()> {
        // `notify_one` keeps the wake-up if the statement has not started waiting yet. Dropping
        // the run stops fetching pages; a page the server is computing finishes there.
        if let Some(running) = self.0.lock().expect("lock poisoned").as_ref() {
            running.notify_one();
        }
        Ok(())
    }
}

mod tls {
    //! TLS with rustls: `require` encrypts without checking the certificate; `verify-ca` and
    //! `verify-full` check it and the server's name, and are refused through a tunnel.

    use std::sync::Arc;

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
    use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
    use spelaion_driver_api::{ConnectParams, DriverError, Result, TlsMode};

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    pub fn config(params: &ConnectParams) -> Result<Option<ClientConfig>> {
        let builder = || {
            ClientConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .map_err(|e| DriverError::Connection(format!("TLS: {e}")))
        };
        match params.tls {
            TlsMode::Disable | TlsMode::Prefer => Ok(None),
            TlsMode::Require => Ok(Some(
                builder()?
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(AnyCertificate))
                    .with_no_client_auth(),
            )),
            TlsMode::VerifyCa | TlsMode::VerifyFull => {
                if params.connect_addr.is_some() {
                    return Err(DriverError::Connection(
                        "Cassandra's client checks the certificate against the address it \
                         connects to, which through a tunnel is this machine's: use `require` \
                         (encrypted, the certificate not checked) or connect without a tunnel"
                            .into(),
                    ));
                }
                let mut roots = RootCertStore::empty();
                match &params.ca_certificate {
                    Some(path) => {
                        for cert in CertificateDer::pem_file_iter(path).map_err(|e| {
                            DriverError::Connection(format!(
                                "the CA certificate {} cannot be read: {e}",
                                path.display()
                            ))
                        })? {
                            let cert =
                                cert.map_err(|e| DriverError::Connection(format!("TLS: {e}")))?;
                            roots
                                .add(cert)
                                .map_err(|e| DriverError::Connection(format!("TLS: {e}")))?;
                        }
                    }
                    None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
                }
                let config = builder()?.with_root_certificates(roots);
                Ok(Some(
                    match (&params.client_certificate, &params.client_key) {
                        (Some(cert), Some(key)) => {
                            let certs = CertificateDer::pem_file_iter(cert)
                                .and_then(|c| c.collect::<std::result::Result<Vec<_>, _>>())
                                .map_err(|e| {
                                    DriverError::Connection(format!(
                                        "the client certificate {} cannot be read: {e}",
                                        cert.display()
                                    ))
                                })?;
                            let key = PrivateKeyDer::from_pem_file(key).map_err(|e| {
                                DriverError::Connection(format!(
                                    "the client key {} cannot be read: {e}",
                                    key.display()
                                ))
                            })?;
                            config
                                .with_client_auth_cert(certs, key)
                                .map_err(|e| DriverError::Connection(format!("TLS: {e}")))?
                        }
                        (None, None) => config.with_no_client_auth(),
                        _ => {
                            return Err(DriverError::Connection(
                                "a client certificate needs its key, and a key its certificate"
                                    .into(),
                            ));
                        }
                    },
                ))
            }
        }
    }

    /// `require`: encrypted, the certificate not checked.
    #[derive(Debug)]
    struct AnyCertificate;

    impl ServerCertVerifier for AnyCertificate {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &provider().signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &provider().signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
}
