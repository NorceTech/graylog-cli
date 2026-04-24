use crate::domain::config::GraylogConfig;
use crate::domain::error::HttpError;
use crate::domain::models::{
    AggregateSearchRequest, AggregateSearchResult, FieldsResult, MessageSearchRequest,
    MessageSearchResult, StreamResult, StreamsResult, SystemResult,
};
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait GraylogGateway: Send + Sync {
    fn base_url(&self) -> &str;

    async fn ping(&self) -> Result<(), HttpError>;

    async fn search_messages(
        &self,
        request: MessageSearchRequest,
    ) -> Result<MessageSearchResult, HttpError>;

    async fn search_aggregate(
        &self,
        request: AggregateSearchRequest,
    ) -> Result<AggregateSearchResult, HttpError>;

    async fn list_streams(&self) -> Result<StreamsResult, HttpError>;

    async fn get_stream(&self, stream_id: String) -> Result<StreamResult, HttpError>;

    async fn system_info(&self) -> Result<SystemResult, HttpError>;

    async fn list_fields(&self) -> Result<FieldsResult, HttpError>;
}

pub trait GraylogGatewayFactory: Send + Sync {
    fn build_from_config(
        &self,
        config: GraylogConfig,
    ) -> Result<Arc<dyn GraylogGateway>, HttpError>;
}
