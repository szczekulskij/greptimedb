// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Datanode configurations

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_config::{Configurable, DEFAULT_DATA_HOME, ENV_VAR_SEP};
use common_options::memory::MemoryOptions;
pub use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_wal::config::DatanodeWalConfig;
use common_workload::{DatanodeWorkloadType, sanitize_workload_types};
use file_engine::config::EngineConfig as FileEngineConfig;
use meta_client::MetaClientOptions;
use metric_engine::config::EngineConfig as MetricEngineConfig;
use mito2::config::MitoConfig;
pub(crate) use object_store::config::ObjectStoreConfig;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;

/// Storage engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StorageConfig {
    /// The working directory of database
    pub data_home: String,
    /// Root directory for standalone SQL access to local files.
    ///
    /// Defaults to `<data_home>/copy` when `data_home` is a local path.
    /// Distributed deployments always disable SQL access to local files.
    pub copy_root: Option<String>,
    #[serde(flatten)]
    pub store: ObjectStoreConfig,
    /// Object storage providers
    pub providers: Vec<ObjectStoreConfig>,
}

impl StorageConfig {
    /// Returns true when the default storage config is a remote object storage service such as AWS S3, etc.
    pub fn is_object_storage(&self) -> bool {
        self.store.is_object_storage()
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_home: DEFAULT_DATA_HOME.to_string(),
            copy_root: None,
            store: ObjectStoreConfig::default(),
            providers: vec![],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct DatanodeOptions {
    pub node_id: Option<u64>,
    pub default_column_prefix: Option<String>,
    pub workload_types: Vec<DatanodeWorkloadType>,
    pub require_lease_before_startup: bool,
    pub init_regions_in_background: bool,
    pub init_regions_parallelism: usize,
    pub grpc: GrpcOptions,
    pub http: HttpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub wal: DatanodeWalConfig,
    pub storage: StorageConfig,
    pub max_concurrent_queries: usize,
    /// Timeout to acquire a permit from the concurrent query limiter when
    /// `max_concurrent_queries` is reached. Only effective when the limiter is enabled.
    #[serde(with = "humantime_serde")]
    pub concurrent_query_limiter_timeout: Duration,
    /// Options for different store engines.
    #[serde(deserialize_with = "deserialize_region_engine")]
    pub region_engine: Vec<RegionEngineConfig>,
    pub logging: LoggingOptions,
    pub enable_telemetry: bool,
    pub tracing: TracingOptions,
    pub query: QueryOptions,
    pub memory: MemoryOptions,

    /// Environment variable keys to read and report in heartbeat messages.
    /// The values of these env vars at startup will be sent to metasrv.
    pub heartbeat_env_vars: Vec<String>,

    /// Deprecated options, please use the new options instead.
    #[deprecated(note = "Please use `grpc.bind_addr` instead.")]
    pub rpc_addr: Option<String>,
    #[deprecated(note = "Please use `grpc.server_addr` instead.")]
    pub rpc_hostname: Option<String>,
    #[deprecated(note = "Please use `grpc.runtime_size` instead.")]
    pub rpc_runtime_size: Option<usize>,
    #[deprecated(note = "Please use `grpc.max_recv_message_size` instead.")]
    pub rpc_max_recv_message_size: Option<ReadableSize>,
    #[deprecated(note = "Please use `grpc.max_send_message_size` instead.")]
    pub rpc_max_send_message_size: Option<ReadableSize>,
}

impl DatanodeOptions {
    /// Sanitize the `DatanodeOptions` to ensure the config is valid.
    pub fn sanitize(&mut self) {
        sanitize_workload_types(&mut self.workload_types);

        if self.storage.is_object_storage() {
            self.storage
                .store
                .cache_config_mut()
                .unwrap()
                .sanitize(&self.storage.data_home);
        }
    }
}

impl Default for DatanodeOptions {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            node_id: None,
            default_column_prefix: None,
            workload_types: vec![DatanodeWorkloadType::Hybrid],
            require_lease_before_startup: false,
            init_regions_in_background: false,
            init_regions_parallelism: 16,
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:3001"),
            http: HttpOptions::default(),
            meta_client: None,
            wal: DatanodeWalConfig::default(),
            storage: StorageConfig::default(),
            max_concurrent_queries: 0,
            concurrent_query_limiter_timeout: Duration::from_millis(100),
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
            logging: LoggingOptions::default(),
            enable_telemetry: true,
            tracing: TracingOptions::default(),
            query: QueryOptions::default(),
            memory: MemoryOptions::default(),
            heartbeat_env_vars: vec![],

            // Deprecated options
            rpc_addr: None,
            rpc_hostname: None,
            rpc_runtime_size: None,
            rpc_max_recv_message_size: None,
            rpc_max_send_message_size: None,
        }
    }
}

impl Configurable for DatanodeOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&[
            "heartbeat_env_vars",
            "meta_client.metasrv_addrs",
            "wal.broker_endpoints",
        ])
    }

    fn apply_env_overrides(
        &mut self,
        env_prefix: &str,
        config_file: Option<&str>,
    ) -> common_config::error::Result<()> {
        apply_region_engine_env_overrides(&mut self.region_engine, env_prefix, config_file)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum RegionEngineConfig {
    #[serde(rename = "mito")]
    Mito(MitoConfig),
    #[serde(rename = "file")]
    File(FileEngineConfig),
    #[serde(rename = "metric")]
    Metric(MetricEngineConfig),
}

/// Applies environment variable overrides to `region_engine` configs.
///
/// Because `config-rs` cannot deep-merge an env-var-produced map into a
/// TOML/JSON-produced sequence for the same key, this function performs a
/// post-deserialization pass that reconstructs the correct field-level merge.
///
/// It re-runs a targeted config-rs merge for each engine with the following
/// source ordering (later sources win):
///
///   defaults → env vars → config file
///
/// This means the **config file has the highest precedence**: TOML-specified
/// fields always win over env vars. Env vars only fill in fields that the
/// config file does not set, and everything else falls back to defaults.
///
/// When no config file is present (or the engine is not mentioned in it),
/// the result is: env-var-specified fields take effect, all other fields
/// receive their `Default` values.
pub fn apply_region_engine_env_overrides(
    region_engine: &mut [RegionEngineConfig],
    env_prefix: &str,
    config_file: Option<&str>,
) -> common_config::error::Result<()> {
    use common_config::error::{LoadLayeredConfigSnafu, SerdeJsonSnafu};
    use config::{Config, Environment, File, FileFormat};
    use snafu::ResultExt;

    // Build the env var prefix for region engine keys.
    // e.g. "GREPTIMEDB_DATANODE__REGION_ENGINE" or "UT_PREFIX__REGION_ENGINE"
    let re_prefix = if env_prefix.is_empty() {
        "REGION_ENGINE".to_string()
    } else {
        format!("{}{}{}", env_prefix, ENV_VAR_SEP, "REGION_ENGINE")
    };

    // Check if any env vars with this prefix exist; skip the work if not.
    let has_region_engine_env =
        std::env::vars().any(|(k, _)| k.to_uppercase().starts_with(&re_prefix.to_uppercase()));
    if !has_region_engine_env {
        return Ok(());
    }

    // If a config file is provided, parse it to extract the region_engine section
    // as a TOML value map. We need the raw TOML so that only fields actually
    // present in the file are used as overrides (fields absent from TOML don't
    // appear, preserving the env var values for those).
    let toml_engines: Option<toml::Value> = config_file.and_then(|path| {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                common_telemetry::warn!(
                    "Failed to re-read config file for region_engine env override: {e}"
                );
                return None;
            }
        };
        let table: toml::Value = match toml::from_str(&content) {
            Ok(t) => t,
            Err(e) => {
                common_telemetry::warn!(
                    "Failed to re-parse config file for region_engine env override: {e}"
                );
                return None;
            }
        };
        table.get("region_engine").cloned()
    });

    for engine_cfg in region_engine.iter_mut() {
        let engine_name = match engine_cfg {
            RegionEngineConfig::Mito(_) => "mito",
            RegionEngineConfig::File(_) => "file",
            RegionEngineConfig::Metric(_) => "metric",
        };

        // The env prefix for this specific engine (uppercase).
        // e.g. "GREPTIMEDB_DATANODE__REGION_ENGINE__MITO"
        let engine_env_prefix =
            format!("{}{}{}", re_prefix, ENV_VAR_SEP, engine_name.to_uppercase());

        // Check if any env vars target this engine specifically.
        let has_engine_env = std::env::vars().any(|(k, _)| {
            k.to_uppercase().starts_with(&format!(
                "{}{}",
                engine_env_prefix.to_uppercase(),
                ENV_VAR_SEP
            ))
        });
        if !has_engine_env {
            continue;
        }

        // Get the default config JSON for this engine.
        let default_json = match engine_cfg {
            RegionEngineConfig::Mito(_) => {
                serde_json::to_string(&MitoConfig::default()).context(SerdeJsonSnafu)?
            }
            RegionEngineConfig::File(_) => {
                serde_json::to_string(&FileEngineConfig::default()).context(SerdeJsonSnafu)?
            }
            RegionEngineConfig::Metric(_) => {
                serde_json::to_string(&MetricEngineConfig::default()).context(SerdeJsonSnafu)?
            }
        };

        let env_source = Environment::default()
            .prefix(&engine_env_prefix)
            .try_parsing(true)
            .separator(ENV_VAR_SEP)
            .ignore_empty(false);

        // Extract the TOML-specified fields for this engine (if any).
        // The TOML `[[region_engine]]` is an array of tables. Each element has a
        // single key (the engine name) mapping to the engine's options.
        let toml_json: Option<String> = toml_engines.as_ref().and_then(|engines| {
            let arr = engines.as_array()?;
            for entry in arr {
                if let Some(engine_table) = entry.get(engine_name) {
                    // Convert the TOML value to JSON for use as a config-rs source.
                    return serde_json::to_string(engine_table).ok();
                }
            }
            None
        });

        // Merge order (later wins): defaults → env vars → TOML (if present).
        // TOML has the highest precedence; env vars only fill in fields that
        // the config file does not specify.
        let mut builder = Config::builder()
            .add_source(File::from_str(&default_json, FileFormat::Json))
            .add_source(env_source);

        if let Some(ref toml_str) = toml_json {
            builder = builder.add_source(File::from_str(toml_str, FileFormat::Json));
        }

        let merged = builder
            .build()
            .and_then(|c| match engine_cfg {
                RegionEngineConfig::Mito(_) => c
                    .try_deserialize::<MitoConfig>()
                    .map(RegionEngineConfig::Mito),
                RegionEngineConfig::File(_) => c
                    .try_deserialize::<FileEngineConfig>()
                    .map(RegionEngineConfig::File),
                RegionEngineConfig::Metric(_) => c
                    .try_deserialize::<MetricEngineConfig>()
                    .map(RegionEngineConfig::Metric),
            })
            .context(LoadLayeredConfigSnafu)?;

        *engine_cfg = merged;
    }

    Ok(())
}

/// Deserializes the `region_engine` option, accepting either the usual sequence
/// form (from TOML array-of-tables, JSON arrays, or the default config source)
/// or a map keyed by engine name.
///
/// The map form is what the layered environment-variable source produces for a
/// path such as `GREPTIMEDB_STANDALONE__REGION_ENGINE__MITO__GLOBAL_WRITE_BUFFER_REJECT_SIZE`,
/// which config-rs expands into `{ region_engine: { mito: { global_write_buffer_reject_size: "4GB" } } }`.
/// Without this, startup fails with `invalid type: map, expected a sequence`.
///
/// When the map form is used, the default engine set (`mito` + `file`) is
/// always preserved — overriding a field in one engine does not drop the other
/// engines. Within an engine, fields not present in the map receive their
/// `Default` values.
pub fn deserialize_region_engine<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<RegionEngineConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use std::fmt;

    use serde::de::{self, MapAccess, SeqAccess, Visitor};

    struct RegionEngineVisitor;

    impl<'de> Visitor<'de> for RegionEngineVisitor {
        type Value = Vec<RegionEngineConfig>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str(
                "a sequence of region engine configs, or a map keyed by engine name \
                 (e.g. `mito`, `file`, `metric`)",
            )
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut engines = Vec::new();
            while let Some(engine) = seq.next_element::<RegionEngineConfig>()? {
                engines.push(engine);
            }
            Ok(engines)
        }

        fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut mito: Option<MitoConfig> = None;
            let mut file: Option<FileEngineConfig> = None;
            let mut metric: Option<MetricEngineConfig> = None;

            while let Some(key) = map.next_key::<String>()? {
                match key.as_str() {
                    "mito" => mito = Some(map.next_value()?),
                    "file" => file = Some(map.next_value()?),
                    "metric" => metric = Some(map.next_value()?),
                    other => {
                        return Err(de::Error::custom(format!(
                            "unknown region engine `{other}`, expected one of `mito`, `file`, `metric`"
                        )));
                    }
                }
            }

            // Preserve the default engine set (`mito` + `file`) and override only
            // the engines specified through the map form. `metric` is only added
            // when explicitly configured, matching the default engine set.
            let mut engines = vec![
                RegionEngineConfig::Mito(mito.unwrap_or_default()),
                RegionEngineConfig::File(file.unwrap_or_default()),
            ];
            if let Some(metric) = metric {
                engines.push(RegionEngineConfig::Metric(metric));
            }
            Ok(engines)
        }
    }

    deserializer.deserialize_any(RegionEngineVisitor)
}

#[cfg(test)]
mod tests {
    use common_base::secrets::ExposeSecret;

    use super::*;

    #[test]
    fn test_toml() {
        let opts = DatanodeOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: DatanodeOptions = toml::from_str(&toml_string).unwrap();
    }

    #[test]
    fn test_secstr() {
        let toml_str = r#"
            [storage]
            type = "S3"
            access_key_id = "access_key_id"
            secret_access_key = "secret_access_key"
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert_eq!(
                    "SecretBox<alloc::string::String>([REDACTED])".to_string(),
                    format!("{:?}", cfg.connection.access_key_id)
                );
                assert_eq!(
                    "access_key_id",
                    cfg.connection.access_key_id.expose_secret()
                );
            }
            _ => unreachable!(),
        }
    }
    #[test]
    fn test_skip_ssl_validation_config() {
        // Test with skip_ssl_validation = true
        let toml_str_true = r#"
            [storage]
            type = "S3"
            [storage.http_client]
            skip_ssl_validation = true
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str_true).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert!(cfg.http_client.skip_ssl_validation);
            }
            _ => panic!("Expected S3 config"),
        }

        // Test with skip_ssl_validation = false
        let toml_str_false = r#"
            [storage]
            type = "S3"
            [storage.http_client]
            skip_ssl_validation = false
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str_false).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert!(!cfg.http_client.skip_ssl_validation);
            }
            _ => panic!("Expected S3 config"),
        }
        // Test default value (should be false)
        let toml_str_default = r#"
            [storage]
            type = "S3"
        "#;
        let opts: DatanodeOptions = toml::from_str(toml_str_default).unwrap();
        match &opts.storage.store {
            ObjectStoreConfig::S3(cfg) => {
                assert!(!cfg.http_client.skip_ssl_validation);
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_cache_config() {
        let toml_str = r#"
            [storage]
            data_home = "test_data_home"
            type = "S3"
            [storage.cache_config]
            enable_read_cache = true
        "#;
        let mut opts: DatanodeOptions = toml::from_str(toml_str).unwrap();
        opts.sanitize();
        assert!(opts.storage.store.cache_config().unwrap().enable_read_cache);
        assert_eq!(
            opts.storage.store.cache_config().unwrap().cache_path,
            "test_data_home"
        );
    }
}
