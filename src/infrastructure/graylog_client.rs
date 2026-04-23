use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Method};
use secrecy::ExposeSecret;
use serde_json::{Map, Value, json};

use crate::application::ports::{GraylogGateway, GraylogGatewayFactory};
use crate::domain::config::{GraylogConfig, StoredConfig};
use crate::domain::error::HttpError;
use crate::domain::models::{
    AggregateSearchRequest, AggregateSearchResult, AggregationType, FieldsResult, JsonObject,
    MessageSearchRequest, MessageSearchResult, NormalizedRow, StreamResult, StreamsResult,
    SystemResult,
};
use crate::domain::timerange::CommandTimerange;

const SEARCH_MESSAGES_PATH: &str = "/api/search/messages";
const SEARCH_AGGREGATE_PATH: &str = "/api/search/aggregate";
const STREAMS_PATH: &str = "/api/streams";
const STREAM_PATH_TEMPLATE: &str = "/api/streams/{id}";
const SYSTEM_PATH: &str = "/api/system";
const SYSTEM_FIELDS_PATH: &str = "/api/system/fields";
const REQUESTED_BY_HEADER: &str = "X-Requested-By";
const REQUESTED_BY_VALUE: &str = "graylog-cli";
const DEFAULT_TERMS_LIMIT: u64 = 10;
const DEFAULT_FALLBACK_GROUPING_LIMIT: u64 = 10_000;

#[derive(Debug, Clone, Copy, Default)]
pub struct ReqwestGraylogGatewayFactory;

impl GraylogGatewayFactory for ReqwestGraylogGatewayFactory {
    fn build_from_config(
        &self,
        config: GraylogConfig,
    ) -> Result<Arc<dyn GraylogGateway>, HttpError> {
        Ok(Arc::new(GraylogClient::from_config(config)?))
    }
}

#[derive(Debug, Clone)]
pub struct GraylogClient {
    http_client: Client,
    config: GraylogConfig,
}

impl GraylogClient {
    pub fn new(http_client: Client, config: GraylogConfig) -> Self {
        Self {
            http_client,
            config,
        }
    }

    pub fn from_config(config: GraylogConfig) -> Result<Self, HttpError> {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .danger_accept_invalid_certs(!config.verify_tls)
            .build()
            .map_err(|error| HttpError::RequestBuild {
                message: format!("failed to construct HTTP client: {error}"),
            })?;

        Ok(Self::new(http_client, config))
    }

    pub fn from_stored(config: StoredConfig) -> Result<Self, HttpError> {
        let runtime_config = config
            .into_runtime()
            .map_err(|error| HttpError::RequestBuild {
                message: format!("invalid stored Graylog config: {error}"),
            })?;

        Self::from_config(runtime_config)
    }

    pub fn http_client(&self) -> &Client {
        &self.http_client
    }

    pub fn config(&self) -> &GraylogConfig {
        &self.config
    }

    pub async fn ping(&self) -> Result<(), HttpError> {
        self.system_info().await.map(|_| ())
    }

    pub async fn search_messages(
        &self,
        request: &MessageSearchRequest,
    ) -> Result<MessageSearchResult, HttpError> {
        let payload = self.message_search_payload(request)?;
        let response = self
            .send_json(Method::POST, SEARCH_MESSAGES_PATH, Some(payload))
            .await?;

        let (messages, mut metadata) = normalize_tabular_response(response)?;
        let total_results = extract_optional_u64(&mut metadata, "total_results");

        Ok(MessageSearchResult {
            messages,
            total_results,
            metadata,
        })
    }

    pub async fn search_aggregate(
        &self,
        request: &AggregateSearchRequest,
    ) -> Result<AggregateSearchResult, HttpError> {
        let exact_payload = self.aggregate_search_payload(request, false)?;
        let response = match self
            .send_json(Method::POST, SEARCH_AGGREGATE_PATH, Some(exact_payload))
            .await
        {
            Ok(response) => response,
            Err(error) if should_retry_aggregate_with_legacy_grouping(request, &error) => {
                let fallback_payload = self.aggregate_search_payload(request, true)?;
                self.send_json(Method::POST, SEARCH_AGGREGATE_PATH, Some(fallback_payload))
                    .await?
            }
            Err(error) => return Err(error),
        };

        let (rows, metadata) = match request.aggregation_type {
            AggregationType::DateHistogram if request.interval.is_some() => {
                normalize_date_histogram_response(response, &request.interval)?
            }
            AggregationType::Cardinality if should_use_cardinality_fallback_rows(request) => {
                normalize_cardinality_response(response, &request.field)?
            }
            _ => normalize_tabular_response(response)?,
        };

        Ok(AggregateSearchResult { rows, metadata })
    }

    pub async fn list_streams(&self) -> Result<StreamsResult, HttpError> {
        let response = self.send_json(Method::GET, STREAMS_PATH, None).await?;
        let (streams, mut metadata) = normalize_named_collection(response, "streams")?;
        let total = extract_optional_u64(&mut metadata, "total");

        Ok(StreamsResult {
            streams,
            total,
            metadata,
        })
    }

    pub async fn get_stream(&self, stream_id: &str) -> Result<StreamResult, HttpError> {
        let stream_id = validate_non_empty("stream_id", stream_id)?;
        let path = STREAM_PATH_TEMPLATE.replace("{id}", stream_id);
        let response = self.send_json(Method::GET, &path, None).await?;

        Ok(StreamResult {
            stream: normalize_stream_response(response)?,
        })
    }

    pub async fn system_info(&self) -> Result<SystemResult, HttpError> {
        let response = self.send_json(Method::GET, SYSTEM_PATH, None).await?;

        Ok(SystemResult {
            system: normalize_object_response(response)?,
        })
    }

    pub async fn list_fields(&self) -> Result<FieldsResult, HttpError> {
        let response = self
            .send_json(Method::GET, SYSTEM_FIELDS_PATH, None)
            .await?;
        let fields = normalize_fields_response(response)?;
        Ok(FieldsResult { fields })
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.config.base_url, path);
        let mut builder = self
            .http_client
            .request(method.clone(), url)
            .basic_auth(self.config.token.expose_secret(), Some("token"));

        if method != Method::GET {
            builder = builder.header(REQUESTED_BY_HEADER, REQUESTED_BY_VALUE);
        }

        builder
    }

    async fn send_json(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, HttpError> {
        let mut request = self.request(method, path);

        if let Some(payload) = body {
            request = request.json(&payload);
        }

        let response = request.send().await.map_err(map_transport_error)?;

        let status = response.status();

        if !status.is_success() {
            let message = response.text().await.ok();

            return Err(HttpError::UnexpectedStatus {
                status: status.as_u16(),
                message: status_message(path, status.as_u16(), message.as_deref()),
            });
        }

        response
            .json::<Value>()
            .await
            .map_err(|error| HttpError::Unavailable {
                message: format!(
                    "Graylog returned a malformed JSON response for endpoint `{path}`: {error}"
                ),
            })
    }

    fn message_search_payload(&self, request: &MessageSearchRequest) -> Result<Value, HttpError> {
        let query = validate_non_empty("query", &request.query)?;
        let sort = validate_non_empty("sort", &request.sort)?;
        let fields = request
            .fields
            .iter()
            .map(|field| {
                validate_non_empty("field", field).map(|value| Value::String(value.to_owned()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let streams = normalize_string_array("stream_id", &request.streams)?;

        let mut payload = Map::new();
        payload.insert("query".to_string(), Value::String(query.to_owned()));
        payload.insert("size".to_string(), json!(request.limit));
        payload.insert("from".to_string(), json!(request.offset));
        payload.insert("sort".to_string(), Value::String(sort.to_owned()));
        payload.insert(
            "sort_order".to_string(),
            Value::String(request.sort_direction.as_api_value().to_string()),
        );

        if let Some(timerange) = request.timerange.as_ref() {
            payload.insert("timerange".to_string(), timerange_to_json(timerange)?);
        }

        if !fields.is_empty() {
            payload.insert("fields".to_string(), Value::Array(fields));
        }

        if !streams.is_empty() {
            payload.insert("streams".to_string(), Value::Array(streams));
        }

        Ok(Value::Object(payload))
    }

    fn aggregate_search_payload(
        &self,
        request: &AggregateSearchRequest,
        legacy_grouping_fallback: bool,
    ) -> Result<Value, HttpError> {
        let query = validate_non_empty("query", &request.query)?;
        let field = validate_non_empty("field", &request.field)?;
        let streams = normalize_string_array("stream_id", &request.streams)?;

        let mut payload = Map::new();
        payload.insert("query".to_string(), Value::String(query.to_owned()));
        if let Some(timerange) = request.timerange.as_ref() {
            payload.insert("timerange".to_string(), timerange_to_json(timerange)?);
        }
        payload.insert(
            "group_by".to_string(),
            Value::Array(self.aggregate_group_by(request, field, legacy_grouping_fallback)?),
        );
        payload.insert(
            "metrics".to_string(),
            Value::Array(self.aggregate_metrics(request, field)),
        );

        if !streams.is_empty() {
            payload.insert("streams".to_string(), Value::Array(streams));
        }

        Ok(Value::Object(payload))
    }

    fn aggregate_group_by(
        &self,
        request: &AggregateSearchRequest,
        field: &str,
        legacy_grouping_fallback: bool,
    ) -> Result<Vec<Value>, HttpError> {
        match request.aggregation_type {
            AggregationType::Terms => Ok(vec![json!({
                "field": field,
                "limit": request.size.unwrap_or(DEFAULT_TERMS_LIMIT),
            })]),
            AggregationType::DateHistogram => {
                let interval = validate_non_empty(
                    "interval",
                    request
                        .interval
                        .as_deref()
                        .ok_or_else(|| HttpError::RequestBuild {
                            message: "date_histogram requires an interval".to_string(),
                        })?,
                )?;

                if legacy_grouping_fallback {
                    Ok(vec![json!({
                        "field": field,
                        "limit": request.size.unwrap_or(DEFAULT_FALLBACK_GROUPING_LIMIT),
                    })])
                } else {
                    Ok(vec![json!({
                        "field": field,
                        "timeunit": interval,
                    })])
                }
            }
            AggregationType::Cardinality => {
                if legacy_grouping_fallback {
                    Ok(vec![json!({
                        "field": field,
                        "limit": request.size.unwrap_or(DEFAULT_FALLBACK_GROUPING_LIMIT),
                    })])
                } else {
                    Ok(Vec::new())
                }
            }
            AggregationType::Stats
            | AggregationType::Min
            | AggregationType::Max
            | AggregationType::Avg
            | AggregationType::Sum => Ok(Vec::new()),
        }
    }

    fn aggregate_metrics(&self, request: &AggregateSearchRequest, field: &str) -> Vec<Value> {
        match request.aggregation_type {
            AggregationType::Terms | AggregationType::DateHistogram => {
                vec![metric_payload(Some("count"), None)]
            }
            AggregationType::Cardinality => vec![metric_payload(
                request.aggregation_type.graylog_metric_name(),
                Some(field),
            )],
            AggregationType::Stats => vec![
                metric_payload(Some("min"), Some(field)),
                metric_payload(Some("max"), Some(field)),
                metric_payload(Some("avg"), Some(field)),
                metric_payload(Some("count"), None),
                metric_payload(Some("stddev"), Some(field)),
            ],
            AggregationType::Min
            | AggregationType::Max
            | AggregationType::Avg
            | AggregationType::Sum => vec![metric_payload(
                request.aggregation_type.graylog_metric_name(),
                Some(field),
            )],
        }
    }
}

fn should_retry_aggregate_with_legacy_grouping(
    request: &AggregateSearchRequest,
    error: &HttpError,
) -> bool {
    match (&request.aggregation_type, error) {
        (
            AggregationType::DateHistogram,
            HttpError::UnexpectedStatus {
                status: 400,
                message,
            },
        ) => {
            message.contains("timeunit")
                || message.contains("Known properties include: field, limit")
        }
        (
            AggregationType::Cardinality,
            HttpError::UnexpectedStatus {
                status: 400,
                message,
            },
        ) => message.contains("groupings") || message.contains("must not be empty"),
        _ => false,
    }
}

fn should_use_cardinality_fallback_rows(request: &AggregateSearchRequest) -> bool {
    matches!(request.aggregation_type, AggregationType::Cardinality)
}

#[async_trait]
impl GraylogGateway for GraylogClient {
    fn base_url(&self) -> &str {
        &self.config.base_url
    }

    async fn ping(&self) -> Result<(), HttpError> {
        GraylogClient::ping(self).await
    }

    async fn search_messages(
        &self,
        request: MessageSearchRequest,
    ) -> Result<MessageSearchResult, HttpError> {
        GraylogClient::search_messages(self, &request).await
    }

    async fn search_aggregate(
        &self,
        request: AggregateSearchRequest,
    ) -> Result<AggregateSearchResult, HttpError> {
        GraylogClient::search_aggregate(self, &request).await
    }

    async fn list_streams(&self) -> Result<StreamsResult, HttpError> {
        GraylogClient::list_streams(self).await
    }

    async fn get_stream(&self, stream_id: String) -> Result<StreamResult, HttpError> {
        GraylogClient::get_stream(self, &stream_id).await
    }

    async fn system_info(&self) -> Result<SystemResult, HttpError> {
        GraylogClient::system_info(self).await
    }

    async fn list_fields(&self) -> Result<FieldsResult, HttpError> {
        GraylogClient::list_fields(self).await
    }
}

fn map_transport_error(error: reqwest::Error) -> HttpError {
    let message = if error.is_timeout() {
        "request to Graylog timed out".to_string()
    } else if error.is_connect() {
        "could not connect to Graylog".to_string()
    } else if error.is_request() {
        "failed to send request to Graylog".to_string()
    } else {
        format!("Graylog transport request failed: {error}")
    };

    HttpError::Transport { message }
}

fn status_message(path: &str, status: u16, body: Option<&str>) -> String {
    match status {
        401 | 403 => "Graylog rejected the supplied credentials".to_string(),
        404 => format!("Graylog endpoint `{path}` is unavailable"),
        405 | 501 => format!("Graylog endpoint `{path}` is not supported"),
        _ => match sanitize_server_message(body) {
            Some(message) => format!("Graylog returned HTTP {status}: {message}"),
            None => format!("Graylog returned HTTP {status}"),
        },
    }
}

fn sanitize_server_message(body: Option<&str>) -> Option<String> {
    let trimmed = body?.trim();

    if trimmed.is_empty() {
        return None;
    }

    let single_line = trimmed.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated = single_line.chars().take(200).collect::<String>();

    if truncated.is_empty() {
        None
    } else {
        Some(truncated)
    }
}

fn metric_payload(function: Option<&str>, field: Option<&str>) -> Value {
    let mut metric = Map::new();

    if let Some(function) = function {
        metric.insert("function".to_string(), Value::String(function.to_string()));
    }

    if let Some(field) = field {
        metric.insert("field".to_string(), Value::String(field.to_string()));
    }

    Value::Object(metric)
}

fn normalize_date_histogram_response(
    value: Value,
    interval: &Option<String>,
) -> Result<(Vec<NormalizedRow>, JsonObject), HttpError> {
    let interval = parse_date_histogram_interval(interval.as_deref().ok_or_else(|| {
        HttpError::RequestBuild {
            message: "date_histogram requires an interval".to_string(),
        }
    })?)?;

    let mut object = match value {
        Value::Object(object) => object,
        other => {
            return Err(HttpError::Unavailable {
                message: format!("expected Graylog object response, got {other}"),
            });
        }
    };

    let schema = object.remove("schema").unwrap_or(Value::Array(Vec::new()));
    let datarows = object
        .remove("datarows")
        .unwrap_or(Value::Array(Vec::new()));
    let mut metadata = match object.remove("metadata") {
        Some(Value::Object(metadata)) => metadata,
        Some(other) => {
            return Err(HttpError::Unavailable {
                message: format!("expected metadata object, got {other}"),
            });
        }
        None => Map::new(),
    };

    metadata.extend(object);

    let columns = extract_schema_columns(schema)?;

    if columns.len() < 2 {
        return Err(HttpError::Unavailable {
            message: "expected date histogram response to include timestamp and count columns"
                .to_string(),
        });
    }

    let rows = match datarows {
        Value::Array(rows) => rows,
        other => {
            return Err(HttpError::Unavailable {
                message: format!("expected datarows array, got {other}"),
            });
        }
    };

    let mut buckets = std::collections::BTreeMap::<String, u64>::new();

    for row in rows {
        let values = match row {
            Value::Array(values) => values,
            other => {
                return Err(HttpError::Unavailable {
                    message: format!("expected datarow array, got {other}"),
                });
            }
        };

        let bucket = bucket_timestamp_value(
            values.first().ok_or_else(|| HttpError::Unavailable {
                message: "expected timestamp value in date histogram response".to_string(),
            })?,
            interval,
        )?;
        let count = extract_count_value(values.get(1).ok_or_else(|| HttpError::Unavailable {
            message: "expected count metric in date histogram response".to_string(),
        })?)?;

        *buckets.entry(bucket).or_insert(0) += count;
    }

    let rows = buckets
        .into_iter()
        .map(|(bucket, count)| {
            let mut row = Map::new();
            row.insert(columns[0].clone(), Value::String(bucket));
            row.insert(columns[1].clone(), json!(count));
            row
        })
        .collect();

    Ok((rows, metadata))
}

fn normalize_cardinality_response(
    value: Value,
    field: &str,
) -> Result<(Vec<NormalizedRow>, JsonObject), HttpError> {
    let (rows, metadata) = normalize_tabular_response(value)?;
    let cardinality = rows.iter().try_fold(0_u64, |accumulator, row| {
        let metric = row.values().find_map(|value| match value {
            Value::Number(number) => Some(number),
            _ => None,
        });

        let metric = metric.ok_or_else(|| HttpError::Unavailable {
            message: "expected numeric metric in cardinality response".to_string(),
        })?;

        let value = metric
            .as_u64()
            .or_else(|| metric.as_i64().map(|value| value as u64))
            .ok_or_else(|| HttpError::Unavailable {
                message: "expected integer cardinality metric in response".to_string(),
            })?;

        Ok(accumulator + value)
    })?;

    let mut row = Map::new();
    row.insert(format!("metric: card({field})"), json!(cardinality));

    Ok((vec![row], metadata))
}

#[derive(Debug, Clone, Copy)]
enum DateHistogramInterval {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Weeks(u32),
    Months(u32),
    Years(u32),
}

fn parse_date_histogram_interval(value: &str) -> Result<DateHistogramInterval, HttpError> {
    let trimmed = validate_non_empty("interval", value)?;
    let lowered = trimmed.to_ascii_lowercase();

    match lowered.as_str() {
        "second" | "seconds" => return Ok(DateHistogramInterval::Seconds(1)),
        "minute" | "minutes" => return Ok(DateHistogramInterval::Minutes(1)),
        "hour" | "hours" => return Ok(DateHistogramInterval::Hours(1)),
        "day" | "days" => return Ok(DateHistogramInterval::Days(1)),
        "week" | "weeks" => return Ok(DateHistogramInterval::Weeks(1)),
        "month" | "months" => return Ok(DateHistogramInterval::Months(1)),
        "quarter" | "quarters" => return Ok(DateHistogramInterval::Months(3)),
        "year" | "years" => return Ok(DateHistogramInterval::Years(1)),
        _ => {}
    }

    if trimmed.len() < 2 {
        return Err(HttpError::RequestBuild {
            message: format!("unsupported `interval` value `{trimmed}` for date_histogram"),
        });
    }

    let (amount, unit) = trimmed.split_at(trimmed.len() - 1);
    let amount = amount.parse::<u32>().map_err(|_| HttpError::RequestBuild {
        message: format!("unsupported `interval` value `{trimmed}` for date_histogram"),
    })?;

    if amount == 0 {
        return Err(HttpError::RequestBuild {
            message: format!("unsupported `interval` value `{trimmed}` for date_histogram"),
        });
    }

    match unit {
        "s" => Ok(DateHistogramInterval::Seconds(amount)),
        "m" => Ok(DateHistogramInterval::Minutes(amount)),
        "h" => Ok(DateHistogramInterval::Hours(amount)),
        "d" => Ok(DateHistogramInterval::Days(amount)),
        "w" => Ok(DateHistogramInterval::Weeks(amount)),
        "M" => Ok(DateHistogramInterval::Months(amount)),
        "y" => Ok(DateHistogramInterval::Years(amount)),
        _ => Err(HttpError::RequestBuild {
            message: format!("unsupported `interval` value `{trimmed}` for date_histogram"),
        }),
    }
}

fn bucket_timestamp_value(
    value: &Value,
    interval: DateHistogramInterval,
) -> Result<String, HttpError> {
    let timestamp = match value {
        Value::String(timestamp) => timestamp,
        other => {
            return Err(HttpError::Unavailable {
                message: format!(
                    "expected timestamp string in date histogram response, got {other}"
                ),
            });
        }
    };

    let mut parts = parse_rfc3339_utc_timestamp(timestamp)?;

    match interval {
        DateHistogramInterval::Seconds(size) => {
            parts.second = floor_component(parts.second, size);
        }
        DateHistogramInterval::Minutes(size) => {
            parts.minute = floor_component(parts.minute, size);
            parts.second = 0;
        }
        DateHistogramInterval::Hours(size) => {
            parts.hour = floor_component(parts.hour, size);
            parts.minute = 0;
            parts.second = 0;
        }
        DateHistogramInterval::Days(size) => {
            let bucket_day = days_from_civil(parts.year, parts.month, parts.day)
                .div_euclid(i64::from(size))
                * i64::from(size);
            let (year, month, day) = civil_from_days(bucket_day);
            parts.year = year;
            parts.month = month;
            parts.day = day;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
        }
        DateHistogramInterval::Weeks(size) => {
            let week_span = i64::from(size) * 7;
            let monday_epoch = days_from_civil(1970, 1, 5);
            let bucket_day = (days_from_civil(parts.year, parts.month, parts.day) - monday_epoch)
                .div_euclid(week_span)
                * week_span
                + monday_epoch;
            let (year, month, day) = civil_from_days(bucket_day);
            parts.year = year;
            parts.month = month;
            parts.day = day;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
        }
        DateHistogramInterval::Months(size) => {
            let total_months = parts.year * 12 + i32::from(parts.month) - 1;
            let bucket_months = total_months.div_euclid(size as i32) * size as i32;
            parts.year = bucket_months.div_euclid(12);
            parts.month = (bucket_months.rem_euclid(12) + 1) as u8;
            parts.day = 1;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
        }
        DateHistogramInterval::Years(size) => {
            parts.year = parts.year.div_euclid(size as i32) * size as i32;
            parts.month = 1;
            parts.day = 1;
            parts.hour = 0;
            parts.minute = 0;
            parts.second = 0;
        }
    }

    Ok(format_timestamp(parts))
}

fn extract_count_value(value: &Value) -> Result<u64, HttpError> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_i64().map(|v| v as u64))
            .ok_or_else(|| HttpError::Unavailable {
                message: format!("expected count metric to be an integer, got {value}"),
            }),
        other => Err(HttpError::Unavailable {
            message: format!("expected count metric to be numeric, got {other}"),
        }),
    }
}

#[derive(Debug, Clone, Copy)]
struct TimestampParts {
    year: i32,
    month: u8,
    day: u8,
    hour: u32,
    minute: u32,
    second: u32,
}

fn parse_rfc3339_utc_timestamp(value: &str) -> Result<TimestampParts, HttpError> {
    if value.len() < 20 || !value.ends_with('Z') {
        return Err(HttpError::Unavailable {
            message: format!("expected RFC3339 UTC timestamp, got {value}"),
        });
    }

    Ok(TimestampParts {
        year: parse_component(value, 0, 4, "year")? as i32,
        month: parse_component(value, 5, 7, "month")? as u8,
        day: parse_component(value, 8, 10, "day")? as u8,
        hour: parse_component(value, 11, 13, "hour")?,
        minute: parse_component(value, 14, 16, "minute")?,
        second: parse_component(value, 17, 19, "second")?,
    })
}

fn parse_component(
    value: &str,
    start: usize,
    end: usize,
    field: &'static str,
) -> Result<u32, HttpError> {
    value
        .get(start..end)
        .ok_or_else(|| HttpError::Unavailable {
            message: format!("expected timestamp component `{field}` in {value}"),
        })?
        .parse::<u32>()
        .map_err(|_| HttpError::Unavailable {
            message: format!("expected numeric timestamp component `{field}` in {value}"),
        })
}

fn floor_component(value: u32, size: u32) -> u32 {
    (value / size) * size
}

fn format_timestamp(parts: TimestampParts) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z",
        parts.year, parts.month, parts.day, parts.hour, parts.minute, parts.second
    )
}

fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month = i32::from(month);
    let day = i32::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;

    i64::from(era) * 146_097 + i64::from(doe) - 719_468
}

fn civil_from_days(days: i64) -> (i32, u8, u8) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096).div_euclid(365);
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2).div_euclid(153);
    let day = doy - (153 * mp + 2).div_euclid(5) + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };

    (
        (year + i64::from(month <= 2)) as i32,
        month as u8,
        day as u8,
    )
}

fn normalize_tabular_response(value: Value) -> Result<(Vec<NormalizedRow>, JsonObject), HttpError> {
    let mut object = match value {
        Value::Object(object) => object,
        other => {
            return Err(HttpError::Unavailable {
                message: format!("expected Graylog object response, got {other}"),
            });
        }
    };

    let schema = object.remove("schema").unwrap_or(Value::Array(Vec::new()));
    let datarows = object
        .remove("datarows")
        .unwrap_or(Value::Array(Vec::new()));
    let mut metadata = match object.remove("metadata") {
        Some(Value::Object(metadata)) => metadata,
        Some(other) => {
            return Err(HttpError::Unavailable {
                message: format!("expected metadata object, got {other}"),
            });
        }
        None => Map::new(),
    };

    metadata.extend(object);

    Ok((normalize_rows(schema, datarows)?, metadata))
}

fn normalize_rows(schema: Value, datarows: Value) -> Result<Vec<NormalizedRow>, HttpError> {
    let columns = extract_schema_columns(schema)?;
    let rows = match datarows {
        Value::Array(rows) => rows,
        other => {
            return Err(HttpError::Unavailable {
                message: format!("expected datarows array, got {other}"),
            });
        }
    };

    rows.into_iter()
        .map(|row| normalize_row(&columns, row))
        .collect()
}

fn extract_schema_columns(schema: Value) -> Result<Vec<String>, HttpError> {
    match schema {
        Value::Array(columns) => columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| match column {
                Value::Object(mut object) => match object.remove("name") {
                    Some(Value::String(name)) if !name.trim().is_empty() => Ok(name),
                    Some(other) => Err(HttpError::Unavailable {
                        message: format!("expected schema column name string, got {other}"),
                    }),
                    None => Ok(format!("column_{index}")),
                },
                other => Err(HttpError::Unavailable {
                    message: format!("expected schema column object, got {other}"),
                }),
            })
            .collect(),
        other => Err(HttpError::Unavailable {
            message: format!("expected schema array, got {other}"),
        }),
    }
}

fn normalize_row(columns: &[String], row: Value) -> Result<NormalizedRow, HttpError> {
    let values = match row {
        Value::Array(values) => values,
        other => {
            return Err(HttpError::Unavailable {
                message: format!("expected datarow array, got {other}"),
            });
        }
    };

    let width = columns.len().max(values.len());
    let mut normalized = Map::with_capacity(width);

    for index in 0..width {
        let key = columns
            .get(index)
            .cloned()
            .unwrap_or_else(|| format!("column_{index}"));
        let value = values.get(index).cloned().unwrap_or(Value::Null);
        normalized.insert(strip_field_prefix(&key), value);
    }

    Ok(normalized)
}

/// Graylog returns search result column names with a "field: " prefix
/// (e.g. "field: message", "field: source"). Strip it for cleaner JSON keys.
fn strip_field_prefix(key: &str) -> String {
    key.strip_prefix("field: ")
        .map(str::to_string)
        .unwrap_or_else(|| key.to_string())
}

fn normalize_named_collection(
    value: Value,
    key: &str,
) -> Result<(Vec<JsonObject>, JsonObject), HttpError> {
    let mut object = normalize_object_response(value)?;
    let collection = object.remove(key).ok_or_else(|| HttpError::Unavailable {
        message: format!("expected `{key}` collection in Graylog response"),
    })?;
    let rows = normalize_object_array(collection)?;

    Ok((rows, object))
}

fn normalize_object_array(value: Value) -> Result<Vec<JsonObject>, HttpError> {
    match value {
        Value::Array(values) => values
            .into_iter()
            .map(|value| match value {
                Value::Object(object) => Ok(object),
                other => Err(HttpError::Unavailable {
                    message: format!("expected object inside collection, got {other}"),
                }),
            })
            .collect(),
        other => Err(HttpError::Unavailable {
            message: format!("expected array response, got {other}"),
        }),
    }
}

fn normalize_object_response(value: Value) -> Result<JsonObject, HttpError> {
    match value {
        Value::Object(object) => Ok(object),
        other => Err(HttpError::Unavailable {
            message: format!("expected JSON object response, got {other}"),
        }),
    }
}

fn normalize_fields_response(value: Value) -> Result<Vec<String>, HttpError> {
    let mut object = normalize_object_response(value)?;
    match object.remove("fields") {
        Some(Value::Array(fields)) => fields
            .into_iter()
            .map(|value| match value {
                Value::String(name) if !name.trim().is_empty() => Ok(name),
                other => Err(HttpError::Unavailable {
                    message: format!("expected field name string, got {other}"),
                }),
            })
            .collect(),
        Some(other) => Err(HttpError::Unavailable {
            message: format!("expected fields array, got {other}"),
        }),
        None => Err(HttpError::Unavailable {
            message: "expected `fields` key in Graylog response".to_string(),
        }),
    }
}

fn normalize_stream_response(value: Value) -> Result<JsonObject, HttpError> {
    let mut object = normalize_object_response(value)?;

    match object.remove("stream") {
        Some(Value::Object(stream)) => Ok(stream),
        Some(other) => Err(HttpError::Unavailable {
            message: format!("expected `stream` object in Graylog response, got {other}"),
        }),
        None => Ok(object),
    }
}

fn extract_optional_u64(map: &mut JsonObject, key: &str) -> Option<u64> {
    match map.remove(key) {
        Some(Value::Number(number)) => number.as_u64(),
        Some(other) => {
            map.insert(key.to_string(), other);
            None
        }
        None => None,
    }
}

fn normalize_string_array(field: &'static str, values: &[String]) -> Result<Vec<Value>, HttpError> {
    values
        .iter()
        .map(|value| validate_non_empty(field, value).map(|value| Value::String(value.to_owned())))
        .collect()
}

fn validate_non_empty<'a>(field: &'static str, value: &'a str) -> Result<&'a str, HttpError> {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return Err(HttpError::RequestBuild {
            message: format!("`{field}` cannot be empty"),
        });
    }

    Ok(trimmed)
}

fn timerange_to_json(timerange: &CommandTimerange) -> Result<Value, HttpError> {
    match timerange {
        CommandTimerange::Relative(relative) => Ok(json!({
            "type": "relative",
            "range": relative.api_range().map_err(|error| HttpError::RequestBuild {
                message: error.to_string(),
            })?,
        })),
        CommandTimerange::Absolute(absolute) => Ok(json!({
            "type": "absolute",
            "from": absolute.from(),
            "to": absolute.to(),
        })),
    }
}
