use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use exn::ResultExt;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::json;
use url::Url;

use crate::application::ports::{CacheStore, ConfigStore, GraylogGateway, GraylogGatewayFactory};
use crate::domain::config::{Config, DEFAULT_PROFILE_NAME, GraylogConfig, validate_profile_name};
use crate::domain::error::{CliError, HttpError, ValidationError};
use crate::domain::models::{
    AggregateCommandInput, AggregateSearchRequest, AggregateStatus, AuthStatus, FieldsStatus,
    MessageSearchRequest, MessageSearchStatus, NormalizedRow, PingStatus, ProfileDeleteStatus,
    ProfileStatus, ProfileSummary, ProfilesStatus, SearchCommandInput, SearchGroup, SortDirection,
    StreamFindStatus, StreamStatus, StreamsStatus, SystemInfoStatus,
};

const DEFAULT_SEARCH_LIMIT: u64 = 50;
const MAX_STREAM_SEARCH_LIMIT: u64 = 100;
const MAX_ALL_PAGES_MESSAGES: usize = 10_000;
const DEFAULT_SEARCH_OFFSET: u64 = 0;
const DEFAULT_SEARCH_SORT: &str = "timestamp";

#[derive(Serialize, Deserialize)]
struct CachedFields {
    fields: Vec<String>,
    fetched_at: u64,
}

#[derive(Clone)]
pub struct ApplicationService {
    config_store: Arc<dyn ConfigStore>,
    gateway_factory: Arc<dyn GraylogGatewayFactory>,
    fields_cache_store: Arc<dyn CacheStore>,
    profile_override: Option<String>,
}

impl ApplicationService {
    pub fn new(
        config_store: Arc<dyn ConfigStore>,
        gateway_factory: Arc<dyn GraylogGatewayFactory>,
        fields_cache_store: Arc<dyn CacheStore>,
    ) -> Self {
        Self {
            config_store,
            gateway_factory,
            fields_cache_store,
            profile_override: None,
        }
    }

    /// Pins every command of this service instance to `profile`, overriding
    /// the persisted `active_profile` selection.
    pub fn with_profile_override(mut self, profile: Option<String>) -> Self {
        self.profile_override = profile;
        self
    }

    pub async fn authenticate(
        &self,
        base_url: Url,
        token: secrecy::SecretString,
    ) -> exn::Result<AuthStatus, CliError> {
        let trimmed_token = token.expose_secret().trim().to_owned();
        if trimmed_token.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField {
                field: "graylog.token",
            })
            .into());
        }

        let profile_name = self
            .profile_override
            .clone()
            .unwrap_or_else(|| DEFAULT_PROFILE_NAME.to_string());
        if let Err(message) = validate_profile_name(&profile_name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message,
            })
            .into());
        }

        let existing = self
            .config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load existing config".to_string()))?;
        let (mut profiles, updater) = existing
            .map(|config| (config.profiles, config.updater))
            .unwrap_or_default();
        if let Some(conflict) = case_conflict(&profiles, &profile_name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message: format!(
                    "profile `{profile_name}` conflicts with existing profile `{conflict}` (names are case-insensitive on some filesystems)"
                ),
            })
            .into());
        }
        profiles.insert(
            profile_name.clone(),
            GraylogConfig::new(
                base_url.clone(),
                secrecy::SecretString::new(trimmed_token.into()),
            ),
        );
        let config = Config {
            profiles,
            active_profile: Some(profile_name.clone()),
            updater,
        };

        self.config_store
            .save(config)
            .await
            .or_raise(|| CliError::Config("failed to persist config".to_string()))?;

        // Re-auth may point the profile at a different server; drop its
        // fields cache so the next command cannot serve the old list.
        let _ = self
            .fields_cache_store
            .remove_serialized(&fields_cache_key(&profile_name))
            .await;

        Ok(AuthStatus::ok(base_url.to_string(), profile_name))
    }

    /// Lists all configured profiles without exposing tokens.
    pub async fn profiles_list(&self) -> exn::Result<ProfilesStatus, CliError> {
        let config = self
            .config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
            .unwrap_or_default();
        // A mistyped --profile/GRAYLOG_PROFILE must fail here too, not
        // surface as a phantom active profile with no summaries.
        if let Some(override_name) = &self.profile_override {
            self.require_profile(&config, override_name)?;
        }
        let active = self.effective_active_profile(&config);

        Ok(ProfilesStatus {
            ok: true,
            command: "profiles.list",
            profiles: config
                .profiles
                .iter()
                .map(|(name, graylog)| profile_summary(name, graylog, active.as_deref()))
                .collect(),
            active_profile: active,
            total: config.profiles.len(),
        })
    }

    /// Shows a single profile; `name` defaults to the resolved active profile.
    pub async fn profiles_show(&self, name: Option<&str>) -> exn::Result<ProfileStatus, CliError> {
        let config = self.load_config().await?;
        if let Some(override_name) = &self.profile_override {
            self.require_profile(&config, override_name)?;
        }
        let (name, graylog) = match name {
            Some(name) => (name.to_string(), self.require_profile(&config, name)?),
            None => self.select_profile(&config)?,
        };
        let active = self
            .effective_active_profile(&config)
            .unwrap_or_else(|| name.clone());

        Ok(ProfileStatus {
            ok: true,
            command: "profiles.show",
            profile: profile_summary(&name, &graylog, Some(active.as_str())),
        })
    }

    /// Switches the persisted active profile.
    pub async fn profiles_use(&self, name: &str) -> exn::Result<ProfileStatus, CliError> {
        if let Err(message) = validate_profile_name(name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message,
            })
            .into());
        }

        let mut config = self.load_config().await?;
        let graylog = self.require_profile(&config, name)?;
        config.active_profile = Some(name.to_string());

        self.config_store
            .save(config)
            .await
            .or_raise(|| CliError::Config("failed to persist config".to_string()))?;

        Ok(ProfileStatus {
            ok: true,
            command: "profiles.use",
            profile: profile_summary(name, &graylog, Some(name)),
        })
    }

    /// Deletes a profile. Deleting the active profile clears `active_profile`;
    /// deleting the last profile leaves an empty map (the not-configured path).
    pub async fn profiles_delete(&self, name: &str) -> exn::Result<ProfileDeleteStatus, CliError> {
        if let Err(message) = validate_profile_name(name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message,
            })
            .into());
        }

        let mut config = self.load_config().await?;
        if config.profiles.remove(name).is_none() {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message: unknown_profile_message(name, &config.profiles),
            })
            .into());
        }
        if config.active_profile.as_deref() == Some(name) {
            config.active_profile = None;
        }

        let remaining_profiles = config.profiles.len();
        let active_profile = config.active_profile.clone();

        self.config_store
            .save(config)
            .await
            .or_raise(|| CliError::Config("failed to persist config".to_string()))?;

        Ok(ProfileDeleteStatus {
            ok: true,
            command: "profiles.delete",
            profile: name.to_string(),
            active_profile,
            remaining_profiles,
        })
    }

    /// Renames a profile, keeping its settings. Follows the active selection:
    /// if the renamed profile was active, the new name becomes active.
    pub async fn profiles_rename(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> exn::Result<ProfileStatus, CliError> {
        if let Err(message) = validate_profile_name(old_name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message,
            })
            .into());
        }
        if let Err(message) = validate_profile_name(new_name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message,
            })
            .into());
        }

        let mut config = self.load_config().await?;
        let graylog = self.require_profile(&config, old_name)?;
        if let Some(conflict) = case_conflict(&config.profiles, new_name) {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message: format!(
                    "profile `{new_name}` conflicts with existing profile `{conflict}` (names are case-insensitive on some filesystems)"
                ),
            })
            .into());
        }
        if config.profiles.contains_key(new_name) {
            let available = config
                .profiles
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message: format!(
                    "profile `{new_name}` already exists (available profiles: {available})"
                ),
            })
            .into());
        }

        config.profiles.remove(old_name);
        config
            .profiles
            .insert(new_name.to_string(), graylog.clone());
        if config.active_profile.as_deref() == Some(old_name) {
            config.active_profile = Some(new_name.to_string());
        }
        let active = self.effective_active_profile(&config);

        self.config_store
            .save(config)
            .await
            .or_raise(|| CliError::Config("failed to persist config".to_string()))?;

        Ok(ProfileStatus {
            ok: true,
            command: "profiles.rename",
            profile: profile_summary(new_name, &graylog, active.as_deref()),
        })
    }

    pub async fn search(
        &self,
        input: SearchCommandInput,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let mut input = input;

        if input.all_fields && input.fields.is_empty() {
            let (profile_name, graylog_config) = self.resolve_profile().await?;
            let ttl = graylog_config.fields_cache_ttl_seconds;
            let cache_key = fields_cache_key(&profile_name);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let fields = match self
                .fields_cache_store
                .get_serialized(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|serialized| serde_json::from_str::<CachedFields>(&serialized).ok())
            {
                Some(cached) if now.saturating_sub(cached.fetched_at) < ttl => cached.fields,
                _ => {
                    let client = self.graylog_gateway_with_config(graylog_config)?;
                    let result = client.list_fields().await.or_raise(|| {
                        CliError::Http(HttpError::Unavailable {
                            message: "failed to list fields".to_string(),
                        })
                    })?;
                    let cache_data = CachedFields {
                        fields: result.fields.clone(),
                        fetched_at: now,
                    };
                    if let Ok(serialized) = serde_json::to_string(&cache_data) {
                        let _ = self
                            .fields_cache_store
                            .save_serialized(cache_key, serialized)
                            .await;
                    }
                    result.fields
                }
            };

            input.fields = fields;
        }

        if let Some(ref group_by) = input.group_by
            && !input.fields.contains(group_by)
        {
            input.fields.push(group_by.clone());
        }

        let group_by = input.group_by.clone();
        let mut status = if input.all_pages {
            self.execute_paginated_search(&input).await?
        } else {
            self.execute_message_search(
                "search",
                self.build_search_request(input, DEFAULT_SEARCH_LIMIT),
            )
            .await?
        };

        if let Some(group_by) = group_by.as_deref() {
            status = apply_grouping(status, group_by);
        }

        Ok(status)
    }

    pub async fn aggregate(
        &self,
        input: AggregateCommandInput,
    ) -> exn::Result<AggregateStatus, CliError> {
        self.execute_aggregate("aggregate", self.build_aggregate_request(input))
            .await
    }

    pub async fn count_by_level(
        &self,
        input: AggregateCommandInput,
    ) -> exn::Result<AggregateStatus, CliError> {
        self.execute_aggregate("count-by-level", self.build_aggregate_request(input))
            .await
    }

    pub async fn streams_list(&self) -> exn::Result<StreamsStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.list_streams().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to list streams".to_string(),
            })
        })?;

        Ok(StreamsStatus {
            ok: true,
            command: "streams.list",
            streams: result.streams,
        })
    }

    pub async fn streams_show(&self, stream_id: &str) -> exn::Result<StreamStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.get_stream(stream_id).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: format!("failed to get stream `{stream_id}`"),
            })
        })?;

        Ok(StreamStatus {
            ok: true,
            command: "streams.show",
            stream: result.stream,
        })
    }

    pub async fn streams_find(&self, name: &str) -> exn::Result<StreamFindStatus, CliError> {
        let name = name.trim();

        if name.is_empty() {
            return Err(CliError::Validation(ValidationError::EmptyField { field: "name" }).into());
        }

        let client = self.graylog_gateway().await?;
        let result = client.list_streams().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to list streams".to_string(),
            })
        })?;
        let needle = name.to_lowercase();
        let streams = result
            .streams
            .into_iter()
            .filter(|stream| {
                stream
                    .get("title")
                    .or_else(|| stream.get("name"))
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_lowercase().contains(&needle))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();

        Ok(StreamFindStatus {
            ok: true,
            command: "streams.find",
            name: name.to_string(),
            returned: streams.len(),
            streams,
        })
    }

    pub async fn streams_search(
        &self,
        input: SearchCommandInput,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let request = self.build_stream_search_request(input)?;
        self.execute_stream_message_search("streams.search", request)
            .await
    }

    pub async fn streams_last_event(
        &self,
        stream_id: String,
        timerange: Option<crate::domain::timerange::CommandTimerange>,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let request = self.build_stream_search_request(SearchCommandInput {
            query: "*".to_string(),
            timerange,
            fields: Vec::new(),
            limit: Some(1),
            offset: Some(DEFAULT_SEARCH_OFFSET),
            sort: Some(DEFAULT_SEARCH_SORT.to_string()),
            sort_direction: Some(SortDirection::Desc),
            group_by: None,
            all_pages: false,
            all_fields: false,
            streams: vec![stream_id],
        })?;

        self.execute_stream_message_search("streams.last-event", request)
            .await
    }

    pub async fn system_info(&self) -> exn::Result<SystemInfoStatus, CliError> {
        let client = self.graylog_gateway().await?;
        let result = client.system_info().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "failed to get system info".to_string(),
            })
        })?;

        Ok(SystemInfoStatus {
            ok: true,
            command: "system.info",
            system: result.system,
        })
    }

    pub async fn fields(&self, refresh: bool) -> exn::Result<FieldsStatus, CliError> {
        let (profile_name, graylog_config) = self.resolve_profile().await?;
        let cache_key = fields_cache_key(&profile_name);
        let ttl = graylog_config.fields_cache_ttl_seconds;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let fetched_fields = if !refresh {
            if let Some(cached) = self
                .fields_cache_store
                .get_serialized(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|s| serde_json::from_str::<CachedFields>(&s).ok())
            {
                if now.saturating_sub(cached.fetched_at) < ttl {
                    Some(cached.fields)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let fields = match fetched_fields {
            Some(fields) => fields,
            None => {
                let client = self.graylog_gateway_with_config(graylog_config)?;
                let result = client.list_fields().await.or_raise(|| {
                    CliError::Http(HttpError::Unavailable {
                        message: "failed to list fields".to_string(),
                    })
                })?;
                let cache_data = CachedFields {
                    fields: result.fields.clone(),
                    fetched_at: now,
                };
                if let Ok(serialized) = serde_json::to_string(&cache_data) {
                    let _ = self
                        .fields_cache_store
                        .save_serialized(cache_key, serialized)
                        .await;
                }
                result.fields
            }
        };

        Ok(FieldsStatus {
            ok: true,
            command: "fields",
            total: fields.len(),
            fields,
        })
    }

    pub async fn ping(&self) -> exn::Result<PingStatus, CliError> {
        let config = self.load_config().await?;
        let available_profiles = config.profiles.keys().cloned().collect::<Vec<_>>();
        let (profile_name, graylog_config) = self.select_profile(&config)?;
        let client = self.graylog_gateway_with_config(graylog_config)?;
        let graylog_url = client.base_url().to_string();

        client.ping().await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "Graylog is unreachable".to_string(),
            })
        })?;

        Ok(PingStatus {
            ok: true,
            command: "ping",
            reachable: true,
            graylog_url,
            profile: profile_name,
            available_profiles,
        })
    }

    fn build_search_request(
        &self,
        input: SearchCommandInput,
        default_limit: u64,
    ) -> MessageSearchRequest {
        MessageSearchRequest {
            query: input.query,
            timerange: input.timerange,
            fields: input.fields,
            limit: input.limit.unwrap_or(default_limit),
            offset: input.offset.unwrap_or(DEFAULT_SEARCH_OFFSET),
            sort: input
                .sort
                .unwrap_or_else(|| DEFAULT_SEARCH_SORT.to_string()),
            sort_direction: input.sort_direction.unwrap_or(SortDirection::Desc),
            streams: input.streams,
        }
    }

    fn build_aggregate_request(&self, input: AggregateCommandInput) -> AggregateSearchRequest {
        AggregateSearchRequest {
            query: input.query,
            timerange: input.timerange,
            aggregation_type: input.aggregation_type,
            field: input.field,
            size: input.size,
            interval: input.interval,
            streams: input.streams,
        }
    }

    fn build_stream_search_request(
        &self,
        input: SearchCommandInput,
    ) -> exn::Result<MessageSearchRequest, CliError> {
        if input.streams.len() != 1 {
            return Err(CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                message: "exactly one stream id is required".to_string(),
            })
            .into());
        }

        let mut request = self.build_search_request(input, DEFAULT_SEARCH_LIMIT);
        request.limit = request.limit.min(MAX_STREAM_SEARCH_LIMIT);
        request.sort = DEFAULT_SEARCH_SORT.to_string();
        request.sort_direction = SortDirection::Desc;

        Ok(request)
    }

    async fn execute_message_search(
        &self,
        command: &'static str,
        request: MessageSearchRequest,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let (_, graylog_config) = self.resolve_profile().await?;
        let client = self.graylog_gateway_with_config(graylog_config)?;
        let result = client.search_messages(request.clone()).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "message search failed".to_string(),
            })
        })?;
        let mut metadata = result.metadata;

        if let Some(total_results) = result.total_results {
            metadata.insert("total_results".to_string(), json!(total_results));
        }

        Ok(MessageSearchStatus {
            ok: true,
            command,
            query: request.query,
            returned: result.messages.len(),
            messages: result.messages,
            grouped_by: None,
            groups: None,
            metadata,
        })
    }

    async fn execute_paginated_search(
        &self,
        input: &SearchCommandInput,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let (_, graylog_config) = self.resolve_profile().await?;
        let client = self.graylog_gateway_with_config(graylog_config)?;
        let mut request = self.build_search_request(input.clone(), DEFAULT_SEARCH_LIMIT);
        let mut all_messages = Vec::new();
        let mut metadata = serde_json::Map::new();
        let mut total_results = None;
        let mut truncated = false;

        request.limit = 500;
        request.offset = 0;

        loop {
            let result = client.search_messages(request.clone()).await.or_raise(|| {
                CliError::Http(HttpError::Unavailable {
                    message: "message search failed".to_string(),
                })
            })?;

            if metadata.is_empty() {
                metadata = result.metadata.clone();
            }

            if total_results.is_none() {
                total_results = result.total_results;
            }

            let fetched = result.messages.len();
            all_messages.extend(result.messages);

            if all_messages.len() >= MAX_ALL_PAGES_MESSAGES {
                truncated = true;
                eprintln!(
                    "warning: --all-pages reached the {MAX_ALL_PAGES_MESSAGES}-message limit; results are truncated"
                );
                break;
            }

            request.offset += fetched as u64;

            if fetched == 0 {
                break;
            }

            match total_results {
                Some(total) => {
                    if request.offset >= total {
                        break;
                    }
                }
                None => {
                    if fetched < request.limit as usize {
                        break;
                    }
                }
            }
        }

        if let Some(total_results) = total_results {
            metadata.insert("total_results".to_string(), json!(total_results));
        }
        if truncated {
            metadata.insert("truncated".to_string(), json!(true));
        }

        Ok(MessageSearchStatus {
            ok: true,
            command: "search",
            query: request.query,
            returned: all_messages.len(),
            messages: all_messages,
            grouped_by: None,
            groups: None,
            metadata,
        })
    }

    async fn execute_stream_message_search(
        &self,
        command: &'static str,
        request: MessageSearchRequest,
    ) -> exn::Result<MessageSearchStatus, CliError> {
        let client = self.graylog_gateway().await?;

        if let Some(stream_id) = request.streams.first() {
            client.get_stream(stream_id).await.or_raise(|| {
                CliError::Http(HttpError::Unavailable {
                    message: format!("failed to get stream `{stream_id}`"),
                })
            })?;
        }

        let result = client.search_messages(request.clone()).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "message search failed".to_string(),
            })
        })?;
        let mut metadata = result.metadata;

        if let Some(total_results) = result.total_results {
            metadata.insert("total_results".to_string(), json!(total_results));
        }

        Ok(MessageSearchStatus {
            ok: true,
            command,
            query: request.query,
            returned: result.messages.len(),
            messages: result.messages,
            grouped_by: None,
            groups: None,
            metadata,
        })
    }

    async fn execute_aggregate(
        &self,
        command: &'static str,
        request: AggregateSearchRequest,
    ) -> exn::Result<AggregateStatus, CliError> {
        let (_, graylog_config) = self.resolve_profile().await?;
        let client = self.graylog_gateway_with_config(graylog_config)?;
        let aggregation_type = request.aggregation_type.as_cli_value();
        let result = client.search_aggregate(request).await.or_raise(|| {
            CliError::Http(HttpError::Unavailable {
                message: "aggregate search failed".to_string(),
            })
        })?;

        Ok(AggregateStatus {
            ok: true,
            command,
            aggregation_type,
            rows: result.rows,
            metadata: result.metadata,
        })
    }

    /// Loads the persisted config, mapping an absent or profile-less config to
    /// a `profile` validation error so the message stays visible in output.
    async fn load_config(&self) -> exn::Result<Config, CliError> {
        self.config_store
            .load()
            .await
            .or_raise(|| CliError::Config("failed to load runtime config".to_string()))?
            .filter(|config| !config.profiles.is_empty())
            .ok_or_else(|| {
                CliError::Validation(ValidationError::InvalidValue {
                    field: "profile",
                    message: "graylog is not configured, run `graylog-cli auth` first".to_string(),
                })
            })
            .map_err(Into::into)
    }

    /// Resolves the profile to use for this invocation: the `--profile`
    /// override first, then the persisted `active_profile`, then the first
    /// stored profile.
    async fn resolve_profile(&self) -> exn::Result<(String, GraylogConfig), CliError> {
        let config = self.load_config().await?;
        self.select_profile(&config).map_err(Into::into)
    }

    fn select_profile(&self, config: &Config) -> Result<(String, GraylogConfig), CliError> {
        let name = self
            .effective_active_profile(config)
            .ok_or_else(|| {
                CliError::Validation(ValidationError::InvalidValue {
                    field: "profile",
                    message: "graylog is not configured, run `graylog-cli auth` first".to_string(),
                })
            })?
            .to_string();
        let graylog = self.require_profile(config, &name)?;
        Ok((name, graylog))
    }

    /// Profile the invocation would use right now, without loading config:
    /// override, persisted active, or first stored profile (BTreeMap order).
    fn effective_active_profile(&self, config: &Config) -> Option<String> {
        self.profile_override
            .clone()
            .or_else(|| config.active_profile.clone())
            .or_else(|| config.profiles.keys().next().cloned())
    }

    fn require_profile(&self, config: &Config, name: &str) -> Result<GraylogConfig, CliError> {
        config.profiles.get(name).cloned().ok_or_else(|| {
            CliError::Validation(ValidationError::InvalidValue {
                field: "profile",
                message: unknown_profile_message(name, &config.profiles),
            })
        })
    }

    async fn graylog_gateway(&self) -> exn::Result<Arc<dyn GraylogGateway>, CliError> {
        let (_, graylog_config) = self.resolve_profile().await?;
        self.graylog_gateway_with_config(graylog_config)
    }

    fn graylog_gateway_with_config(
        &self,
        config: GraylogConfig,
    ) -> exn::Result<Arc<dyn GraylogGateway>, CliError> {
        self.gateway_factory.build_from_config(config).or_raise(|| {
            CliError::Http(HttpError::RequestBuild {
                message: "failed to build Graylog client from config".to_string(),
            })
        })
    }
}

fn profile_summary(name: &str, graylog: &GraylogConfig, active: Option<&str>) -> ProfileSummary {
    ProfileSummary {
        name: name.to_string(),
        url: graylog.url.to_string(),
        timeout_seconds: graylog.timeout_seconds,
        verify_tls: graylog.verify_tls,
        fields_cache_ttl_seconds: graylog.fields_cache_ttl_seconds,
        active: active == Some(name),
    }
}

fn unknown_profile_message(name: &str, profiles: &BTreeMap<String, GraylogConfig>) -> String {
    if profiles.is_empty() {
        "graylog is not configured, run `graylog-cli auth` first".to_string()
    } else {
        let available = profiles.keys().cloned().collect::<Vec<_>>().join(", ");
        format!("unknown profile `{name}`, available profiles: {available}")
    }
}

/// Per-profile fields cache key. A hyphen (not a colon) keeps the resulting
/// cache file name valid on Windows.
fn fields_cache_key(profile: &str) -> String {
    format!("fields-{profile}")
}

/// Existing profile name that differs from `name` only by case, if any.
/// Profile names are case-sensitive map keys, but the fields cache files
/// live on filesystems that may not be (default macOS/Windows), so
/// case-only distinct names would silently share one cache file.
fn case_conflict(profiles: &BTreeMap<String, GraylogConfig>, name: &str) -> Option<String> {
    profiles
        .keys()
        .find(|key| key.eq_ignore_ascii_case(name) && key.as_str() != name)
        .cloned()
}

fn apply_grouping(mut status: MessageSearchStatus, group_by: &str) -> MessageSearchStatus {
    status.grouped_by = Some(group_by.to_string());
    status.groups = Some(build_search_groups(&status.messages, group_by));
    status
}

fn build_search_groups(messages: &[NormalizedRow], group_by: &str) -> Vec<SearchGroup> {
    let mut groups: BTreeMap<String, Vec<&NormalizedRow>> = BTreeMap::new();

    for row in messages {
        let key = row
            .get(group_by)
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_string();
        groups.entry(key).or_default().push(row);
    }

    groups
        .into_iter()
        .map(|(key, rows)| SearchGroup {
            key,
            count: rows.len(),
            duration_ms: compute_group_duration(&rows),
        })
        .collect()
}

fn compute_group_duration(rows: &[&NormalizedRow]) -> Option<u64> {
    let first_ts = rows
        .first()
        .and_then(|row| row.get("timestamp").and_then(|value| value.as_str()))?;
    let last_ts = rows
        .last()
        .and_then(|row| row.get("timestamp").and_then(|value| value.as_str()))?;
    let first_millis = parse_timestamp_to_millis(first_ts)?;
    let last_millis = parse_timestamp_to_millis(last_ts)?;
    Some(first_millis.abs_diff(last_millis))
}

fn parse_timestamp_to_millis(ts: &str) -> Option<i64> {
    use time::format_description::well_known::Rfc3339;
    let dt = time::OffsetDateTime::parse(ts, &Rfc3339).ok()?;
    Some(dt.unix_timestamp() * 1000 + dt.millisecond() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::{Map, Value};

    use crate::application::ports::config_store::ConfigError;
    use crate::application::test_support::fakes::FakeCacheStore;
    use crate::domain::config::{
        DEFAULT_FIELDS_CACHE_TTL_SECONDS, DEFAULT_PROFILE_NAME, DEFAULT_TIMEOUT_SECONDS,
        UpdaterConfig,
    };
    use crate::domain::models::{
        AggregateSearchResult, AggregationType, FieldsResult, JsonObject, MessageSearchResult,
        StreamResult, StreamsResult, SystemResult,
    };
    use crate::domain::timerange::CommandTimerange;

    #[derive(Clone)]
    struct FakeConfigStore {
        state: Arc<Mutex<Option<Config>>>,
    }

    impl FakeConfigStore {
        fn new(config: Config) -> Self {
            Self {
                state: Arc::new(Mutex::new(Some(config))),
            }
        }

        fn empty() -> Self {
            Self {
                state: Arc::new(Mutex::new(None)),
            }
        }

        fn saved_config(&self) -> Option<Config> {
            self.state
                .lock()
                .expect("config mutex should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ConfigStore for FakeConfigStore {
        async fn load(&self) -> exn::Result<Option<Config>, ConfigError> {
            Ok(self
                .state
                .lock()
                .expect("config mutex should not be poisoned")
                .clone())
        }

        async fn save(&self, config: Config) -> exn::Result<(), ConfigError> {
            *self
                .state
                .lock()
                .expect("config mutex should not be poisoned") = Some(config);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FakeGraylogGateway {
        search_results: Arc<Mutex<Vec<MessageSearchResult>>>,
        fields_result: Arc<Mutex<FieldsResult>>,
        streams_result: Arc<Mutex<StreamsResult>>,
        stream_result: Arc<Mutex<StreamResult>>,
        ping_result: Arc<Mutex<Result<(), HttpError>>>,
        system_result: Arc<Mutex<SystemResult>>,
        aggregate_result: Arc<Mutex<AggregateSearchResult>>,
        search_requests: Arc<Mutex<Vec<MessageSearchRequest>>>,
        aggregate_requests: Arc<Mutex<Vec<AggregateSearchRequest>>>,
        list_fields_calls: Arc<Mutex<usize>>,
    }

    impl FakeGraylogGateway {
        fn new() -> Self {
            Self {
                search_results: Arc::new(Mutex::new(vec![make_search_messages_result(
                    Vec::new(),
                    Some(0),
                    Map::new(),
                )])),
                fields_result: Arc::new(Mutex::new(FieldsResult { fields: Vec::new() })),
                streams_result: Arc::new(Mutex::new(StreamsResult {
                    streams: Vec::new(),
                    total: Some(0),
                    metadata: Map::new(),
                })),
                stream_result: Arc::new(Mutex::new(StreamResult { stream: Map::new() })),
                ping_result: Arc::new(Mutex::new(Ok(()))),
                system_result: Arc::new(Mutex::new(SystemResult { system: Map::new() })),
                aggregate_result: Arc::new(Mutex::new(AggregateSearchResult {
                    rows: Vec::new(),
                    metadata: Map::new(),
                })),
                search_requests: Arc::new(Mutex::new(Vec::new())),
                aggregate_requests: Arc::new(Mutex::new(Vec::new())),
                list_fields_calls: Arc::new(Mutex::new(0)),
            }
        }

        fn with_search_results(results: Vec<MessageSearchResult>) -> Self {
            let gateway = Self::new();
            *gateway
                .search_results
                .lock()
                .expect("search result mutex should not be poisoned") = results;
            gateway
        }

        fn set_fields(&self, fields: Vec<String>) {
            *self
                .fields_result
                .lock()
                .expect("fields mutex should not be poisoned") = FieldsResult { fields };
        }

        fn set_streams(&self, streams: Vec<JsonObject>) {
            *self
                .streams_result
                .lock()
                .expect("streams mutex should not be poisoned") = StreamsResult {
                total: Some(streams.len() as u64),
                streams,
                metadata: Map::new(),
            };
        }

        fn set_stream(&self, stream: JsonObject) {
            *self
                .stream_result
                .lock()
                .expect("stream mutex should not be poisoned") = StreamResult { stream };
        }

        fn set_system(&self, system: JsonObject) {
            *self
                .system_result
                .lock()
                .expect("system mutex should not be poisoned") = SystemResult { system };
        }

        fn set_aggregate(&self, rows: Vec<NormalizedRow>, metadata: JsonObject) {
            *self
                .aggregate_result
                .lock()
                .expect("aggregate mutex should not be poisoned") =
                AggregateSearchResult { rows, metadata };
        }

        fn search_requests(&self) -> Vec<MessageSearchRequest> {
            self.search_requests
                .lock()
                .expect("search request mutex should not be poisoned")
                .clone()
        }

        fn aggregate_requests(&self) -> Vec<AggregateSearchRequest> {
            self.aggregate_requests
                .lock()
                .expect("aggregate request mutex should not be poisoned")
                .clone()
        }

        fn list_fields_call_count(&self) -> usize {
            *self
                .list_fields_calls
                .lock()
                .expect("field call mutex should not be poisoned")
        }
    }

    #[async_trait]
    impl GraylogGateway for FakeGraylogGateway {
        fn base_url(&self) -> &str {
            "http://localhost:9000"
        }

        async fn ping(&self) -> Result<(), HttpError> {
            self.ping_result
                .lock()
                .expect("ping mutex should not be poisoned")
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.clone())
        }

        async fn search_messages(
            &self,
            request: MessageSearchRequest,
        ) -> Result<MessageSearchResult, HttpError> {
            self.search_requests
                .lock()
                .expect("search request mutex should not be poisoned")
                .push(request);
            let mut results = self
                .search_results
                .lock()
                .expect("search result mutex should not be poisoned");
            if results.is_empty() {
                Ok(make_search_messages_result(Vec::new(), Some(0), Map::new()))
            } else {
                Ok(results.remove(0))
            }
        }

        async fn search_aggregate(
            &self,
            request: AggregateSearchRequest,
        ) -> Result<AggregateSearchResult, HttpError> {
            self.aggregate_requests
                .lock()
                .expect("aggregate request mutex should not be poisoned")
                .push(request);
            Ok(self
                .aggregate_result
                .lock()
                .expect("aggregate mutex should not be poisoned")
                .clone())
        }

        async fn list_streams(&self) -> Result<StreamsResult, HttpError> {
            Ok(self
                .streams_result
                .lock()
                .expect("streams mutex should not be poisoned")
                .clone())
        }

        async fn get_stream(&self, _stream_id: &str) -> Result<StreamResult, HttpError> {
            Ok(self
                .stream_result
                .lock()
                .expect("stream mutex should not be poisoned")
                .clone())
        }

        async fn system_info(&self) -> Result<SystemResult, HttpError> {
            Ok(self
                .system_result
                .lock()
                .expect("system mutex should not be poisoned")
                .clone())
        }

        async fn list_fields(&self) -> Result<FieldsResult, HttpError> {
            *self
                .list_fields_calls
                .lock()
                .expect("field call mutex should not be poisoned") += 1;
            Ok(self
                .fields_result
                .lock()
                .expect("fields mutex should not be poisoned")
                .clone())
        }
    }

    #[derive(Clone)]
    struct FakeGraylogGatewayFactory {
        gateway: Arc<dyn GraylogGateway>,
        failure: Option<String>,
        built_configs: Arc<Mutex<Vec<GraylogConfig>>>,
    }

    impl FakeGraylogGatewayFactory {
        fn new(gateway: Arc<dyn GraylogGateway>) -> Self {
            Self {
                gateway,
                failure: None,
                built_configs: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// (base_url, token) of each config the factory was asked to build.
        fn built_configs(&self) -> Vec<(String, String)> {
            self.built_configs
                .lock()
                .expect("built config mutex should not be poisoned")
                .iter()
                .map(|config| {
                    (
                        config.url.to_string(),
                        config.token.expose_secret().to_string(),
                    )
                })
                .collect()
        }
    }

    impl GraylogGatewayFactory for FakeGraylogGatewayFactory {
        fn build_from_config(
            &self,
            config: GraylogConfig,
        ) -> Result<Arc<dyn GraylogGateway>, HttpError> {
            if let Some(message) = &self.failure {
                Err(HttpError::RequestBuild {
                    message: message.clone(),
                })
            } else {
                self.built_configs
                    .lock()
                    .expect("built config mutex should not be poisoned")
                    .push(config);
                Ok(Arc::clone(&self.gateway))
            }
        }
    }

    fn test_config() -> Config {
        single_profile_config(DEFAULT_PROFILE_NAME, "http://localhost:9000", "test-token")
    }

    fn single_profile_config(name: &str, url: &str, token: &str) -> Config {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            name.to_string(),
            GraylogConfig::new(
                Url::parse(url).expect("test URL should parse"),
                secrecy::SecretString::new(token.to_owned().into()),
            ),
        );
        Config {
            profiles,
            active_profile: Some(name.to_string()),
            updater: UpdaterConfig::default(),
        }
    }

    fn multi_profile_config() -> Config {
        let mut config = single_profile_config("alpha", "http://alpha:9000", "alpha-token");
        config.profiles.insert(
            "beta".to_string(),
            GraylogConfig::new(
                Url::parse("http://beta:9000").expect("test URL should parse"),
                secrecy::SecretString::new("beta-token".to_owned().into()),
            ),
        );
        config
    }

    fn test_service(
        config_store: Arc<dyn ConfigStore>,
        cache_store: Arc<dyn CacheStore>,
        gateway_factory: Arc<dyn GraylogGatewayFactory>,
    ) -> ApplicationService {
        ApplicationService::new(config_store, gateway_factory, cache_store)
    }

    fn service_with_gateway(
        config_store: FakeConfigStore,
        cache_store: FakeCacheStore,
        gateway: FakeGraylogGateway,
    ) -> (ApplicationService, FakeGraylogGateway, FakeCacheStore) {
        let (service, gateway, cache_store, _) =
            service_with_gateway_and_profile(config_store, cache_store, gateway, None);
        (service, gateway, cache_store)
    }

    fn service_with_gateway_and_profile(
        config_store: FakeConfigStore,
        cache_store: FakeCacheStore,
        gateway: FakeGraylogGateway,
        profile_override: Option<&str>,
    ) -> (
        ApplicationService,
        FakeGraylogGateway,
        FakeCacheStore,
        FakeGraylogGatewayFactory,
    ) {
        let factory = FakeGraylogGatewayFactory::new(Arc::new(gateway.clone()));
        let service = test_service(
            Arc::new(config_store),
            Arc::new(cache_store.clone()),
            Arc::new(factory.clone()),
        )
        .with_profile_override(profile_override.map(str::to_string));
        (service, gateway, cache_store, factory)
    }

    fn make_search_messages_result(
        messages: Vec<NormalizedRow>,
        total_results: Option<u64>,
        metadata: JsonObject,
    ) -> MessageSearchResult {
        MessageSearchResult {
            messages,
            total_results,
            metadata,
        }
    }

    fn make_search_input() -> SearchCommandInput {
        SearchCommandInput {
            query: "source:app".to_string(),
            timerange: None,
            fields: Vec::new(),
            limit: None,
            offset: None,
            sort: None,
            sort_direction: None,
            group_by: None,
            all_pages: false,
            all_fields: false,
            streams: Vec::new(),
        }
    }

    fn make_aggregate_input() -> AggregateCommandInput {
        AggregateCommandInput {
            query: "source:app".to_string(),
            timerange: None,
            aggregation_type: AggregationType::Terms,
            field: "level".to_string(),
            size: None,
            interval: None,
            streams: Vec::new(),
        }
    }

    fn object(entries: Vec<(&str, Value)>) -> JsonObject {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn rows(count: usize) -> Vec<NormalizedRow> {
        (0..count)
            .map(|index| object(vec![("message", json!(format!("message-{index}")))]))
            .collect()
    }

    fn assert_empty_field(error: exn::Exn<CliError>, expected_field: &'static str) {
        assert!(
            matches!(&*error, CliError::Validation(ValidationError::EmptyField { field }) if *field == expected_field),
            "expected EmptyField for {expected_field}, got {error:?}"
        );
    }

    fn assert_profile_validation_error(error: exn::Exn<CliError>, expected: &str) {
        assert!(
            matches!(
                &*error,
                CliError::Validation(ValidationError::InvalidValue { field: "profile", message })
                    if message.contains(expected)
            ),
            "expected InvalidValue profile error containing {expected}, got {error:?}"
        );
    }

    #[tokio::test]
    async fn authenticate_rejects_empty_token() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("".to_owned().into()),
            )
            .await
            .expect_err("empty token should be rejected");
        assert_empty_field(error, "graylog.token");
    }

    #[tokio::test]
    async fn authenticate_rejects_whitespace_only_token() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("  ".to_owned().into()),
            )
            .await
            .expect_err("whitespace token should be rejected");
        assert_empty_field(error, "graylog.token");
    }

    #[tokio::test]
    async fn authenticate_persists_config_with_defaults() {
        let config_store = FakeConfigStore::empty();
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let url = Url::parse("http://localhost:9000").expect("test URL should parse");
        service
            .authenticate(
                url.clone(),
                secrecy::SecretString::new("test-token".to_owned().into()),
            )
            .await
            .expect("authentication should persist config");
        let saved = config_store
            .saved_config()
            .expect("config should be saved after authentication");
        let profile = &saved.profiles[DEFAULT_PROFILE_NAME];
        assert_eq!(profile.url, url);
        assert_eq!(profile.token.expose_secret(), "test-token");
        assert_eq!(profile.timeout_seconds, DEFAULT_TIMEOUT_SECONDS);
        assert!(profile.verify_tls);
        assert_eq!(
            profile.fields_cache_ttl_seconds,
            DEFAULT_FIELDS_CACHE_TTL_SECONDS
        );
        assert_eq!(saved.active_profile.as_deref(), Some(DEFAULT_PROFILE_NAME));
    }

    #[tokio::test]
    async fn authenticate_preserves_existing_updater_settings() {
        let mut seed = test_config();
        seed.updater.disable_auto_update = true;
        let config_store = FakeConfigStore::new(seed);
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("new-token".to_owned().into()),
            )
            .await
            .expect("authentication should succeed");
        let saved = config_store
            .saved_config()
            .expect("config should be saved after authentication");
        assert!(saved.updater.disable_auto_update);
        assert_eq!(
            saved.profiles[DEFAULT_PROFILE_NAME].token.expose_secret(),
            "new-token"
        );
    }

    #[tokio::test]
    async fn authenticate_writes_named_profile_and_preserves_other_profiles() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _, _) = service_with_gateway_and_profile(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("staging"),
        );
        let status = service
            .authenticate(
                Url::parse("http://staging:9000").expect("test URL should parse"),
                secrecy::SecretString::new("staging-token".to_owned().into()),
            )
            .await
            .expect("authentication should succeed");
        assert_eq!(status.profile, "staging");
        assert_eq!(status.graylog_url, "http://staging:9000/");
        let saved = config_store
            .saved_config()
            .expect("config should be saved after authentication");
        assert_eq!(saved.profiles.len(), 3);
        assert_eq!(
            saved.profiles["staging"].token.expose_secret(),
            "staging-token"
        );
        assert!(saved.profiles.contains_key("alpha"));
        assert!(saved.profiles.contains_key("beta"));
        assert_eq!(saved.active_profile.as_deref(), Some("staging"));
    }

    #[tokio::test]
    async fn authenticate_rejects_invalid_profile_name() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("-leading-dash"),
        );
        let error = service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("token".to_owned().into()),
            )
            .await
            .expect_err("invalid profile name should be rejected");
        assert_profile_validation_error(error, "profile names must");
    }

    #[tokio::test]
    async fn authenticate_returns_ok_status_with_url() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let url = Url::parse("http://localhost:9000").expect("test URL should parse");
        let status = service
            .authenticate(
                url.clone(),
                secrecy::SecretString::new("test-token".to_owned().into()),
            )
            .await
            .expect("authentication should succeed");
        assert!(status.ok);
        assert_eq!(status.graylog_url, url.to_string());
        assert_eq!(status.profile, DEFAULT_PROFILE_NAME);
    }

    #[tokio::test]
    async fn search_returns_config_error_when_not_authenticated() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .search(make_search_input())
            .await
            .expect_err("search should require config");
        assert_profile_validation_error(error, "graylog is not configured");
    }

    #[tokio::test]
    async fn search_rejects_unknown_profile_override() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("gamma"),
        );
        let error = service
            .search(make_search_input())
            .await
            .expect_err("unknown profile override should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
    }

    #[tokio::test]
    async fn profile_override_takes_precedence_over_active_profile() {
        let (service, _, _, factory) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("beta"),
        );
        service
            .search(make_search_input())
            .await
            .expect("search should succeed with override");
        let built = factory.built_configs();
        assert_eq!(
            built,
            vec![("http://beta:9000/".to_string(), "beta-token".to_string())]
        );
    }

    #[tokio::test]
    async fn without_override_the_active_profile_is_used() {
        let (service, _, _, factory) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            None,
        );
        service
            .search(make_search_input())
            .await
            .expect("search should succeed with active profile");
        let built = factory.built_configs();
        assert_eq!(
            built,
            vec![("http://alpha:9000/".to_string(), "alpha-token".to_string())]
        );
    }

    #[tokio::test]
    async fn search_builds_default_request() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        service
            .search(make_search_input())
            .await
            .expect("search should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one search request should be recorded");
        assert_eq!(request.limit, 50);
        assert_eq!(request.offset, 0);
        assert_eq!(request.sort, "timestamp");
        assert_eq!(request.sort_direction, SortDirection::Desc);
    }

    #[tokio::test]
    async fn search_preserves_explicit_values() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let timerange = CommandTimerange::relative("15m").expect("relative timerange should parse");
        let mut input = make_search_input();
        input.query = "level:ERROR".to_string();
        input.fields = vec!["message".to_string(), "source".to_string()];
        input.limit = Some(10);
        input.offset = Some(5);
        input.sort = Some("source".to_string());
        input.sort_direction = Some(SortDirection::Asc);
        input.timerange = Some(timerange.clone());
        input.streams = vec!["stream-1".to_string(), "stream-2".to_string()];
        service.search(input).await.expect("search should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one search request should be recorded");
        assert_eq!(request.query, "level:ERROR");
        assert_eq!(request.fields, vec!["message", "source"]);
        assert_eq!(request.limit, 10);
        assert_eq!(request.offset, 5);
        assert_eq!(request.sort, "source");
        assert_eq!(request.sort_direction, SortDirection::Asc);
        assert_eq!(request.timerange, Some(timerange));
        assert_eq!(request.streams, vec!["stream-1", "stream-2"]);
    }

    #[tokio::test]
    async fn search_includes_total_results_in_metadata() {
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            Vec::new(),
            Some(100),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .search(make_search_input())
            .await
            .expect("search should succeed");
        assert_eq!(status.metadata.get("total_results"), Some(&json!(100)));
    }

    #[tokio::test]
    async fn search_with_group_by_injects_field_into_fields() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.fields = vec!["message".to_string()];
        input.group_by = Some("level".to_string());
        service.search(input).await.expect("search should succeed");
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "level"]
        );
    }

    #[tokio::test]
    async fn search_with_group_by_does_not_duplicate_field() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.fields = vec!["message".to_string(), "level".to_string()];
        input.group_by = Some("level".to_string());
        service.search(input).await.expect("search should succeed");
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "level"]
        );
    }

    #[tokio::test]
    async fn search_with_group_by_returns_grouped_output() {
        let messages = vec![
            object(vec![("level", json!("ERROR"))]),
            object(vec![("level", json!("ERROR"))]),
            object(vec![("level", json!("ERROR"))]),
            object(vec![("level", json!("WARN"))]),
            object(vec![("level", json!("WARN"))]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(5),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.grouped_by, Some("level".to_string()));
        let groups = status.groups.expect("groups should be present");
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].key, "ERROR");
        assert_eq!(groups[0].count, 3);
        assert_eq!(groups[1].key, "WARN");
        assert_eq!(groups[1].count, 2);
    }

    #[tokio::test]
    async fn search_groups_compute_duration_from_timestamps() {
        let messages = vec![
            object(vec![
                ("level", json!("ERROR")),
                ("timestamp", json!("2026-01-01T00:00:00Z")),
            ]),
            object(vec![
                ("level", json!("ERROR")),
                ("timestamp", json!("2026-01-01T00:00:02.500Z")),
            ]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(2),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(
            status.groups.expect("groups should be present")[0].duration_ms,
            Some(2_500)
        );
    }

    #[tokio::test]
    async fn search_groups_compute_duration_from_descending_timestamps() {
        let messages = vec![
            object(vec![
                ("level", json!("ERROR")),
                ("timestamp", json!("2026-01-01T00:00:02.500Z")),
            ]),
            object(vec![
                ("level", json!("ERROR")),
                ("timestamp", json!("2026-01-01T00:00:00Z")),
            ]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(2),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(
            status.groups.expect("groups should be present")[0].duration_ms,
            Some(2_500)
        );
    }

    #[tokio::test]
    async fn search_groups_unknown_bucket_for_missing_field() {
        let messages = vec![
            object(vec![("message", json!("first"))]),
            object(vec![("message", json!("second"))]),
        ];
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            messages,
            Some(2),
            Map::new(),
        )]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.group_by = Some("level".to_string());
        let status = service.search(input).await.expect("search should succeed");
        let groups = status.groups.expect("groups should be present");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "unknown");
        assert_eq!(groups[0].count, 2);
    }

    #[tokio::test]
    async fn search_all_pages_fetches_all_pages() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), Some(1250), Map::new()),
            make_search_messages_result(rows(500), Some(1250), Map::new()),
            make_search_messages_result(rows(250), Some(1250), Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        let requests = gateway.search_requests();
        assert_eq!(status.messages.len(), 1_250);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].offset, 0);
        assert_eq!(requests[1].offset, 500);
        assert_eq!(requests[2].offset, 1_000);
        assert!(requests.iter().all(|request| request.limit == 500));
    }

    #[tokio::test]
    async fn search_all_pages_stops_on_empty_page() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), None, Map::new()),
            make_search_messages_result(Vec::new(), None, Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.messages.len(), 500);
        assert_eq!(gateway.search_requests().len(), 2);
    }

    #[tokio::test]
    async fn search_all_pages_stops_on_short_page() {
        let gateway = FakeGraylogGateway::with_search_results(vec![make_search_messages_result(
            rows(300),
            None,
            Map::new(),
        )]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.messages.len(), 300);
        assert_eq!(gateway.search_requests().len(), 1);
    }

    #[tokio::test]
    async fn search_all_pages_continues_past_short_page_when_total_known() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), Some(1_250), Map::new()),
            make_search_messages_result(rows(250), Some(1_250), Map::new()),
            make_search_messages_result(rows(500), Some(1_250), Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        let requests = gateway.search_requests();
        assert_eq!(status.messages.len(), 1_250);
        assert_eq!(
            requests
                .iter()
                .map(|request| request.offset)
                .collect::<Vec<_>>(),
            vec![0, 500, 750]
        );
        assert!(requests.iter().all(|request| request.limit == 500));
    }

    #[tokio::test]
    async fn search_all_pages_stops_on_total_reached() {
        let gateway = FakeGraylogGateway::with_search_results(vec![
            make_search_messages_result(rows(500), Some(600), Map::new()),
            make_search_messages_result(rows(100), Some(600), Map::new()),
        ]);
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.all_pages = true;
        let status = service.search(input).await.expect("search should succeed");
        assert_eq!(status.messages.len(), 600);
        assert_eq!(gateway.search_requests().len(), 2);
    }

    #[tokio::test]
    async fn search_all_fields_uses_cached_fields_on_hit() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["uncached".to_string()]);
        let cache_store = FakeCacheStore::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs();
        cache_store.insert(
            "fields-default",
            serde_json::to_string(&CachedFields {
                fields: vec!["message".to_string(), "source".to_string()],
                fetched_at: now,
            })
            .expect("cached fields should serialize"),
        );
        let (service, gateway, _) =
            service_with_gateway(FakeConfigStore::new(test_config()), cache_store, gateway);
        let mut input = make_search_input();
        input.all_fields = true;
        service.search(input).await.expect("search should succeed");
        assert_eq!(gateway.list_fields_call_count(), 0);
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "source"]
        );
    }

    #[tokio::test]
    async fn search_all_fields_fetches_and_caches_on_miss() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["message".to_string(), "level".to_string()]);
        let cache_store = FakeCacheStore::default();
        let (service, gateway, cache_store) =
            service_with_gateway(FakeConfigStore::new(test_config()), cache_store, gateway);
        let mut input = make_search_input();
        input.all_fields = true;
        service.search(input).await.expect("search should succeed");
        assert_eq!(gateway.list_fields_call_count(), 1);
        assert!(cache_store.get("fields-default").is_some());
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["message", "level"]
        );
    }

    #[tokio::test]
    async fn search_all_fields_refetches_on_expired_cache() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["fresh".to_string()]);
        let cache_store = FakeCacheStore::default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_secs();
        cache_store.insert(
            "fields-default",
            serde_json::to_string(&CachedFields {
                fields: vec!["stale".to_string()],
                fetched_at: now - DEFAULT_FIELDS_CACHE_TTL_SECONDS - 1,
            })
            .expect("cached fields should serialize"),
        );
        let (service, gateway, _) =
            service_with_gateway(FakeConfigStore::new(test_config()), cache_store, gateway);
        let mut input = make_search_input();
        input.all_fields = true;
        service.search(input).await.expect("search should succeed");
        assert_eq!(gateway.list_fields_call_count(), 1);
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one search request should be recorded")
                .fields,
            vec!["fresh"]
        );
    }

    #[tokio::test]
    async fn aggregate_builds_request_from_input() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let timerange = CommandTimerange::relative("1h").expect("relative timerange should parse");
        let mut input = make_aggregate_input();
        input.query = "level:ERROR".to_string();
        input.timerange = Some(timerange.clone());
        input.aggregation_type = AggregationType::DateHistogram;
        input.field = "timestamp".to_string();
        input.size = Some(25);
        input.interval = Some("minute".to_string());
        input.streams = vec!["stream-1".to_string()];
        service
            .aggregate(input)
            .await
            .expect("aggregate should succeed");
        let requests = gateway.aggregate_requests();
        let request = requests
            .first()
            .expect("one aggregate request should be recorded");
        assert_eq!(request.query, "level:ERROR");
        assert_eq!(request.timerange, Some(timerange));
        assert_eq!(request.aggregation_type, AggregationType::DateHistogram);
        assert_eq!(request.field, "timestamp");
        assert_eq!(request.size, Some(25));
        assert_eq!(request.interval, Some("minute".to_string()));
        assert_eq!(request.streams, vec!["stream-1"]);
    }

    #[tokio::test]
    async fn aggregate_returns_aggregate_status() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_aggregate(
            vec![object(vec![("level", json!("ERROR")), ("count", json!(2))])],
            object(vec![("source", json!("aggregate"))]),
        );
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .aggregate(make_aggregate_input())
            .await
            .expect("aggregate should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "aggregate");
        assert_eq!(status.aggregation_type, "terms");
        assert_eq!(status.rows.len(), 1);
    }

    #[tokio::test]
    async fn count_by_level_returns_correct_command() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .count_by_level(make_aggregate_input())
            .await
            .expect("count-by-level should succeed");
        assert_eq!(status.command, "count-by-level");
    }

    #[tokio::test]
    async fn aggregate_returns_config_error_when_not_authenticated() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .aggregate(make_aggregate_input())
            .await
            .expect_err("aggregate should require config");
        assert_profile_validation_error(error, "graylog is not configured");
    }

    #[tokio::test]
    async fn streams_find_rejects_empty_name() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .streams_find("")
            .await
            .expect_err("empty name should fail");
        assert_empty_field(error, "name");
    }

    #[tokio::test]
    async fn streams_find_rejects_whitespace_name() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .streams_find("  ")
            .await
            .expect_err("whitespace name should fail");
        assert_empty_field(error, "name");
    }

    #[tokio::test]
    async fn streams_find_filters_case_insensitively_by_title() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![
            object(vec![("title", json!("All Errors"))]),
            object(vec![("title", json!("error logs"))]),
            object(vec![("title", json!("Info"))]),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_find("error")
            .await
            .expect("find should succeed");
        assert_eq!(status.returned, 2);
        assert_eq!(status.streams.len(), 2);
    }

    #[tokio::test]
    async fn streams_find_matches_by_name_when_title_absent() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![
            object(vec![("name", json!("error logs"))]),
            object(vec![("name", json!("info logs"))]),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_find("error")
            .await
            .expect("find should succeed");
        assert_eq!(status.returned, 1);
        assert_eq!(status.streams[0].get("name"), Some(&json!("error logs")));
    }

    #[tokio::test]
    async fn streams_find_trims_input() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![object(vec![("title", json!("error logs"))])]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_find("  error  ")
            .await
            .expect("find should succeed");
        assert_eq!(status.name, "error");
        assert_eq!(status.returned, 1);
    }

    #[tokio::test]
    async fn streams_search_requires_exactly_one_stream_id() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let no_stream_error = service
            .streams_search(make_search_input())
            .await
            .expect_err("missing stream id should fail");
        assert!(matches!(
            &*no_stream_error,
            CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                ..
            })
        ));
        let mut input = make_search_input();
        input.streams = vec!["stream-1".to_string(), "stream-2".to_string()];
        let two_stream_error = service
            .streams_search(input)
            .await
            .expect_err("multiple stream ids should fail");
        assert!(matches!(
            &*two_stream_error,
            CliError::Validation(ValidationError::InvalidValue {
                field: "stream_id",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn streams_search_caps_limit_at_100() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.streams = vec!["stream-1".to_string()];
        input.limit = Some(200);
        service
            .streams_search(input)
            .await
            .expect("stream search should succeed");
        assert_eq!(
            gateway
                .search_requests()
                .first()
                .expect("one stream search request should be recorded")
                .limit,
            100
        );
    }

    #[tokio::test]
    async fn streams_search_forces_sort_timestamp_desc() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let mut input = make_search_input();
        input.streams = vec!["stream-1".to_string()];
        input.sort = Some("source".to_string());
        input.sort_direction = Some(SortDirection::Asc);
        service
            .streams_search(input)
            .await
            .expect("stream search should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one stream search request should be recorded");
        assert_eq!(request.sort, "timestamp");
        assert_eq!(request.sort_direction, SortDirection::Desc);
    }

    #[tokio::test]
    async fn streams_last_event_builds_default_request() {
        let gateway = FakeGraylogGateway::new();
        let (service, gateway, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        service
            .streams_last_event("stream-1".to_string(), None)
            .await
            .expect("last event should succeed");
        let requests = gateway.search_requests();
        let request = requests
            .first()
            .expect("one stream search request should be recorded");
        assert_eq!(request.query, "*");
        assert_eq!(request.limit, 1);
        assert_eq!(request.offset, 0);
        assert_eq!(request.sort, "timestamp");
        assert_eq!(request.sort_direction, SortDirection::Desc);
        assert_eq!(request.streams, vec!["stream-1"]);
    }

    #[tokio::test]
    async fn streams_list_returns_all_streams() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_streams(vec![
            object(vec![("id", json!("1"))]),
            object(vec![("id", json!("2"))]),
            object(vec![("id", json!("3"))]),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_list()
            .await
            .expect("streams list should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "streams.list");
        assert_eq!(status.streams.len(), 3);
    }

    #[tokio::test]
    async fn streams_show_returns_single_stream() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_stream(object(vec![("id", json!("stream-1"))]));
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .streams_show("stream-1")
            .await
            .expect("streams show should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "streams.show");
        assert_eq!(status.stream.get("id"), Some(&json!("stream-1")));
    }

    #[tokio::test]
    async fn system_info_returns_gateway_payload() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_system(object(vec![("version", json!("6.0.0"))]));
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service
            .system_info()
            .await
            .expect("system info should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "system.info");
        assert_eq!(status.system.get("version"), Some(&json!("6.0.0")));
    }

    #[tokio::test]
    async fn fields_returns_list_and_count() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec![
            "message".to_string(),
            "source".to_string(),
            "level".to_string(),
        ]);
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            gateway,
        );
        let status = service.fields(false).await.expect("fields should succeed");
        assert!(status.ok);
        assert_eq!(status.fields, vec!["message", "source", "level"]);
        assert_eq!(status.total, 3);
    }

    #[tokio::test]
    async fn fields_cache_is_scoped_per_profile() {
        let gateway = FakeGraylogGateway::new();
        gateway.set_fields(vec!["beta-field".to_string()]);
        let cache_store = FakeCacheStore::default();
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            cache_store.clone(),
            gateway,
            Some("beta"),
        );
        service
            .fields(false)
            .await
            .expect("fields should succeed for beta");
        assert!(cache_store.get("fields-beta").is_some());
        assert!(cache_store.get("fields-alpha").is_none());
        assert!(cache_store.get("fields").is_none());
    }

    #[tokio::test]
    async fn ping_returns_reachable_status() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(test_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service.ping().await.expect("ping should succeed");
        assert!(status.ok);
        assert!(status.reachable);
        assert_eq!(status.graylog_url, "http://localhost:9000");
        assert_eq!(status.profile, DEFAULT_PROFILE_NAME);
        assert_eq!(status.available_profiles, vec![DEFAULT_PROFILE_NAME]);
    }

    #[tokio::test]
    async fn ping_reports_override_and_available_profiles() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("beta"),
        );
        let status = service.ping().await.expect("ping should succeed");
        assert_eq!(status.profile, "beta");
        assert_eq!(status.available_profiles, vec!["alpha", "beta"]);
    }

    // --- Profile management tests ---

    #[tokio::test]
    async fn profiles_list_returns_summaries_without_tokens() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _) = service_with_gateway(
            config_store,
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_list()
            .await
            .expect("profiles list should succeed");
        assert!(status.ok);
        assert_eq!(status.command, "profiles.list");
        assert_eq!(status.total, 2);
        assert_eq!(status.active_profile.as_deref(), Some("alpha"));
        assert_eq!(status.profiles.len(), 2);
        assert_eq!(status.profiles[0].name, "alpha");
        assert!(status.profiles[0].active);
        assert_eq!(status.profiles[0].url, "http://alpha:9000/");
        assert_eq!(status.profiles[1].name, "beta");
        assert!(!status.profiles[1].active);

        let serialized = serde_json::to_string(&status).expect("status should serialize");
        assert!(!serialized.contains("alpha-token"));
        assert!(!serialized.contains("beta-token"));
    }

    #[test]
    fn profile_summary_serialization_contains_no_token_field() {
        let summary = ProfileSummary {
            name: "prod".to_string(),
            url: "https://graylog.example.com/".to_string(),
            timeout_seconds: 60,
            verify_tls: true,
            fields_cache_ttl_seconds: 300,
            active: true,
        };

        let serialized = serde_json::to_string(&summary).expect("summary should serialize");

        assert!(!serialized.contains("token"));
        let mut keys = serde_json::from_str::<serde_json::Value>(&serialized)
            .expect("summary should parse")
            .as_object()
            .expect("summary should be an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "active",
                "fields_cache_ttl_seconds",
                "name",
                "timeout_seconds",
                "url",
                "verify_tls"
            ]
        );
    }

    #[tokio::test]
    async fn profiles_list_with_empty_store_returns_empty_status() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::empty(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_list()
            .await
            .expect("profiles list should succeed without config");
        assert_eq!(status.total, 0);
        assert!(status.profiles.is_empty());
        assert_eq!(status.active_profile, None);
    }

    #[tokio::test]
    async fn profiles_show_defaults_to_resolved_active_profile() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_show(None)
            .await
            .expect("profiles show should succeed");
        assert_eq!(status.command, "profiles.show");
        assert_eq!(status.profile.name, "alpha");
        assert!(status.profile.active);
        let serialized = serde_json::to_string(&status).expect("status should serialize");
        assert!(!serialized.contains("alpha-token"));
    }

    #[tokio::test]
    async fn profiles_show_with_override_reports_override_as_active() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("beta"),
        );
        let status = service
            .profiles_show(None)
            .await
            .expect("profiles show should succeed");
        assert_eq!(status.profile.name, "beta");
        assert!(status.profile.active);
    }

    #[tokio::test]
    async fn profiles_show_named_profile() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_show(Some("beta"))
            .await
            .expect("profiles show should succeed");
        assert_eq!(status.profile.name, "beta");
        assert!(!status.profile.active);
    }

    #[tokio::test]
    async fn profiles_show_rejects_unknown_profile() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_show(Some("gamma"))
            .await
            .expect_err("unknown profile should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
    }

    #[tokio::test]
    async fn profiles_list_rejects_unknown_override() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("gamma"),
        );
        let error = service
            .profiles_list()
            .await
            .expect_err("unknown override should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
    }

    #[tokio::test]
    async fn profiles_show_named_rejects_unknown_override() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("gamma"),
        );
        let error = service
            .profiles_show(Some("alpha"))
            .await
            .expect_err("unknown override should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
    }

    #[tokio::test]
    async fn profiles_use_switches_active_profile() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_use("beta")
            .await
            .expect("profiles use should succeed");
        assert_eq!(status.command, "profiles.use");
        assert_eq!(status.profile.name, "beta");
        assert!(status.profile.active);
        let saved = config_store.saved_config().expect("config should be saved");
        assert_eq!(saved.active_profile.as_deref(), Some("beta"));
        assert_eq!(saved.profiles.len(), 2);
    }

    #[tokio::test]
    async fn profiles_use_rejects_unknown_profile() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_use("gamma")
            .await
            .expect_err("unknown profile should fail");
        let message = match (&*error, "capture") {
            (
                CliError::Validation(ValidationError::InvalidValue {
                    field: "profile",
                    message,
                }),
                _,
            ) => message.clone(),
            _ => panic!("expected InvalidValue profile error, got {error:?}"),
        };
        assert!(message.contains("unknown profile `gamma`"), "got {message}");
        assert!(
            message.contains("available profiles: alpha, beta"),
            "got {message}"
        );
    }

    #[tokio::test]
    async fn profiles_use_rejects_invalid_profile_name() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_use("no spaces")
            .await
            .expect_err("invalid profile name should fail");
        assert_profile_validation_error(error, "profile names must");
    }

    #[tokio::test]
    async fn profiles_delete_rejects_invalid_profile_name() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_delete("no spaces")
            .await
            .expect_err("invalid profile name should fail");
        assert_profile_validation_error(error, "profile names must");
    }

    #[tokio::test]
    async fn profiles_rename_moves_settings_and_keeps_active() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_rename("beta", "gamma")
            .await
            .expect("profiles rename should succeed");
        assert_eq!(status.command, "profiles.rename");
        assert_eq!(status.profile.name, "gamma");
        assert_eq!(status.profile.url, "http://beta:9000/");
        let saved = config_store.saved_config().expect("config should be saved");
        assert!(!saved.profiles.contains_key("beta"));
        assert!(saved.profiles.contains_key("gamma"));
        assert_eq!(saved.active_profile.as_deref(), Some("alpha"));
    }

    #[tokio::test]
    async fn profiles_rename_follows_active_profile() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        service
            .profiles_rename("alpha", "gamma")
            .await
            .expect("profiles rename should succeed");
        let saved = config_store.saved_config().expect("config should be saved");
        assert_eq!(saved.active_profile.as_deref(), Some("gamma"));
    }

    #[tokio::test]
    async fn profiles_rename_rejects_unknown_and_existing_names() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_rename("gamma", "delta")
            .await
            .expect_err("unknown source should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
        let error = service
            .profiles_rename("alpha", "beta")
            .await
            .expect_err("existing target should fail");
        assert_profile_validation_error(error, "already exists");
        let error = service
            .profiles_rename("no spaces", "delta")
            .await
            .expect_err("invalid source should fail");
        assert_profile_validation_error(error, "profile names must");
    }

    #[tokio::test]
    async fn authenticate_invalidates_profile_fields_cache() {
        let cache_store = FakeCacheStore::default();
        cache_store.insert("fields-default", "{\"stale\":true}".to_string());
        let (service, _, cache_store) = service_with_gateway(
            FakeConfigStore::empty(),
            cache_store,
            FakeGraylogGateway::new(),
        );
        service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("test-token".to_owned().into()),
            )
            .await
            .expect("authentication should succeed");
        assert!(cache_store.get("fields-default").is_none());
    }

    #[tokio::test]
    async fn authenticate_rejects_case_conflicting_profile_name() {
        let (service, _, _, _) = service_with_gateway_and_profile(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            Some("ALPHA"),
        );
        let error = service
            .authenticate(
                Url::parse("http://localhost:9000").expect("test URL should parse"),
                secrecy::SecretString::new("test-token".to_owned().into()),
            )
            .await
            .expect_err("case-conflicting profile should fail");
        assert_profile_validation_error(error, "conflicts with existing profile `alpha`");
    }

    #[tokio::test]
    async fn profiles_rename_rejects_case_conflicting_target() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_rename("beta", "ALPHA")
            .await
            .expect_err("case-conflicting target should fail");
        assert_profile_validation_error(error, "conflicts with existing profile `alpha`");
    }

    #[tokio::test]
    async fn profiles_delete_inactive_profile_keeps_active() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_delete("beta")
            .await
            .expect("profiles delete should succeed");
        assert_eq!(status.command, "profiles.delete");
        assert_eq!(status.profile, "beta");
        assert_eq!(status.remaining_profiles, 1);
        assert_eq!(status.active_profile.as_deref(), Some("alpha"));
        let saved = config_store.saved_config().expect("config should be saved");
        assert!(!saved.profiles.contains_key("beta"));
        assert_eq!(saved.active_profile.as_deref(), Some("alpha"));
    }

    #[tokio::test]
    async fn profiles_delete_active_profile_clears_active_profile() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_delete("alpha")
            .await
            .expect("profiles delete should succeed");
        assert_eq!(status.active_profile, None);
        assert_eq!(status.remaining_profiles, 1);
        let saved = config_store.saved_config().expect("config should be saved");
        assert_eq!(saved.active_profile, None);
        assert!(saved.profiles.contains_key("beta"));
    }

    #[tokio::test]
    async fn deleting_active_profile_falls_back_to_remaining_profile() {
        let config_store = FakeConfigStore::new(multi_profile_config());
        let (service, _, _, factory) = service_with_gateway_and_profile(
            config_store,
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
            None,
        );
        service
            .profiles_delete("alpha")
            .await
            .expect("profiles delete should succeed");
        service
            .search(make_search_input())
            .await
            .expect("search should fall back to the remaining profile");
        let built = factory.built_configs();
        assert_eq!(
            built,
            vec![("http://beta:9000/".to_string(), "beta-token".to_string())]
        );
    }

    #[tokio::test]
    async fn profiles_delete_last_profile_leaves_empty_not_configured() {
        let config_store = FakeConfigStore::new(test_config());
        let (service, _, _) = service_with_gateway(
            config_store.clone(),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let status = service
            .profiles_delete(DEFAULT_PROFILE_NAME)
            .await
            .expect("profiles delete should succeed");
        assert_eq!(status.remaining_profiles, 0);
        assert_eq!(status.active_profile, None);
        let saved = config_store.saved_config().expect("config should be saved");
        assert!(saved.profiles.is_empty());

        let error = service
            .search(make_search_input())
            .await
            .expect_err("commands after deleting the last profile are not configured");
        assert_profile_validation_error(error, "graylog is not configured");
    }

    #[tokio::test]
    async fn profiles_delete_rejects_unknown_profile() {
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(multi_profile_config()),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .profiles_delete("gamma")
            .await
            .expect_err("unknown profile should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
    }

    #[tokio::test]
    async fn commands_fail_when_active_profile_is_dangling() {
        let mut config = multi_profile_config();
        config.active_profile = Some("gamma".to_string());
        let (service, _, _) = service_with_gateway(
            FakeConfigStore::new(config),
            FakeCacheStore::default(),
            FakeGraylogGateway::new(),
        );
        let error = service
            .search(make_search_input())
            .await
            .expect_err("dangling active profile should fail");
        assert_profile_validation_error(error, "unknown profile `gamma`");
    }
}
