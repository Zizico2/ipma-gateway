//! Policy layer over the shared IPMA API. All data comes from a gRPC adapter.
//! The initial policy is bounded caching; authentication/rate limiting can be
//! added here without changing the adapter or duplicating the wire contract.

use ipma_proto::{
    MAX_MESSAGE_BYTES,
    v1::{
        self as pb, proxy_service_client::ProxyServiceClient, proxy_service_server::ProxyService,
    },
};
use moka::{Expiry, future::Cache};
use prost::Message;
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Semaphore;
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
    metadata::{KeyAndValueRef, MetadataMap},
    transport::{Channel, ClientTlsConfig, Endpoint},
};

#[derive(Debug, Clone)]
pub struct GatewayOptions {
    pub upstream_address: String,
    pub upstream_timeout: Duration,
    pub cache_capacity_bytes: u64,
    pub cache_ttl_cap: Option<Duration>,
    pub max_concurrent_forwards: usize,
    /// Trusted ingress/tracing metadata that does not vary public response data.
    /// Only affects cache eligibility; metadata is still forwarded unchanged.
    pub cache_neutral_metadata: Vec<String>,
}
impl Default for GatewayOptions {
    fn default() -> Self {
        Self {
            upstream_address: "http://127.0.0.1:50051".into(),
            upstream_timeout: Duration::from_secs(15),
            cache_capacity_bytes: 64 * 1024 * 1024,
            cache_ttl_cap: None,
            max_concurrent_forwards: 8,
            cache_neutral_metadata: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CacheKey {
    method: &'static str,
    request: Vec<u8>,
}
#[derive(Clone, Copy)]
struct Policy {
    ttl: Duration,
    day_relative: bool,
}
impl Policy {
    const fn seconds(seconds: u64) -> Self {
        Self {
            ttl: Duration::from_secs(seconds),
            day_relative: false,
        }
    }
    const fn daily() -> Self {
        Self {
            ttl: Duration::from_secs(600),
            day_relative: true,
        }
    }
}

#[derive(Debug)]
struct Cached {
    body: Body,
    metadata: MetadataMap,
    expires: Instant,
}
#[derive(Debug)]
enum LoadError {
    Upstream(Status),
    Uncacheable(Arc<Cached>),
}
struct CacheExpiry;
impl Expiry<CacheKey, Arc<Cached>> for CacheExpiry {
    fn expire_after_create(
        &self,
        _: &CacheKey,
        value: &Arc<Cached>,
        now: Instant,
    ) -> Option<Duration> {
        Some(value.expires.saturating_duration_since(now))
    }
}

#[derive(Clone)]
pub struct Gateway {
    client: ProxyServiceClient<Channel>,
    options: GatewayOptions,
    cache: Cache<CacheKey, Arc<Cached>>,
    permits: Arc<Semaphore>,
}
impl Gateway {
    pub async fn connect(options: GatewayOptions) -> anyhow::Result<Self> {
        anyhow::ensure!(
            options.cache_capacity_bytes > 0
                && options.max_concurrent_forwards > 0
                && options.max_concurrent_forwards <= 256,
            "cache budget and forward capacity must be positive; forward capacity must not exceed 256"
        );
        anyhow::ensure!(
            !options.upstream_timeout.is_zero()
                && options.upstream_timeout <= Duration::from_secs(300),
            "upstream timeout must be in (0, 300s]"
        );
        anyhow::ensure!(
            !options.cache_ttl_cap.is_some_and(|ttl| ttl.is_zero()),
            "cache TTL cap must be positive"
        );
        for name in &options.cache_neutral_metadata {
            anyhow::ensure!(
                !name.is_empty()
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                    && !name.ends_with("-bin")
                    && !matches!(
                        name.as_str(),
                        "authorization"
                            | "proxy-authorization"
                            | "cookie"
                            | "x-api-key"
                            | "x-tenant"
                            | "x-tenant-id"
                            | "tenant-id"
                    ),
                "cache-neutral metadata must be lowercase ASCII names and cannot include credentials, tenant IDs or binary metadata"
            );
        }
        let uri: http::Uri = options.upstream_address.parse()?;
        anyhow::ensure!(
            matches!(uri.scheme_str(), Some("http" | "https"))
                && uri.authority().is_some_and(|a| !a.as_str().contains('@'))
                && uri.path_and_query().is_none_or(|p| p.as_str() == "/"),
            "upstream must be an HTTP(S) origin without credentials, path or query"
        );
        let mut endpoint = Endpoint::from_shared(options.upstream_address.clone())?
            .connect_timeout(options.upstream_timeout.min(Duration::from_secs(5)))
            .tcp_nodelay(true);
        if uri.scheme_str() == Some("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        let channel = endpoint.connect().await?;
        let client = ProxyServiceClient::new(channel)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(64 * 1024);
        let cache = Cache::builder()
            .max_capacity(options.cache_capacity_bytes)
            .weigher(|key: &CacheKey, value: &Arc<Cached>| {
                value
                    .body
                    .encoded_len()
                    .saturating_add(key.request.len())
                    .saturating_mul(4)
                    .saturating_add(2048)
                    .min(u32::MAX as usize) as u32
            })
            .expire_after(CacheExpiry)
            .build();
        let permits = Arc::new(Semaphore::new(options.max_concurrent_forwards));
        Ok(Self {
            client,
            options,
            cache,
            permits,
        })
    }

    async fn forward<Req, Res, F, Fut>(
        &self,
        mut request: Request<Req>,
        budget: Duration,
        call: F,
    ) -> Result<Response<Res>, Status>
    where
        F: FnOnce(ProxyServiceClient<Channel>, Request<Req>) -> Fut,
        Fut: Future<Output = Result<Response<Res>, Status>>,
    {
        if budget.is_zero() {
            return Err(Status::deadline_exceeded("request deadline elapsed"));
        }
        let _permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("gateway forward capacity exhausted"))?;
        strip_transport_metadata(request.metadata_mut());
        request.set_timeout(budget);
        let mut response = tokio::time::timeout(budget, call(self.client.clone(), request))
            .await
            .map_err(|_| Status::deadline_exceeded("adapter request timed out"))??;
        strip_transport_metadata(response.metadata_mut());
        Ok(response)
    }

    async fn cached_call<Req, Res, F, Fut>(
        &self,
        request: Request<Req>,
        method: &'static str,
        policy: Policy,
        call: F,
    ) -> Result<Response<Res>, Status>
    where
        Req: Message + Send + 'static,
        Res: Cacheable,
        F: FnOnce(ProxyServiceClient<Channel>, Request<Req>) -> Fut + Send,
        Fut: Future<Output = Result<Response<Res>, Status>> + Send,
    {
        let budget = request_budget(request.metadata(), self.options.upstream_timeout)?;
        if budget.is_zero() {
            return Err(Status::deadline_exceeded("request deadline elapsed"));
        }
        // Caller context is isolated from public cache entries.
        if bypass_cache(request.metadata(), &self.options.cache_neutral_metadata) {
            return self.forward(request, budget, call).await;
        }
        let explicit_deadline = request.metadata().contains_key("grpc-timeout");
        let key = CacheKey {
            method,
            request: request.get_ref().encode_to_vec(),
        };
        let request_len = key.request.len();
        let day_boundary = if policy.day_relative {
            let now = SystemTime::now();
            Some(now + until_midnight(now))
        } else {
            None
        };
        // Reuse public hits, but do not bind a shared cache initializer to one
        // caller's short deadline. Deadline-bearing misses forward individually.
        if explicit_deadline {
            if let Some(value) = self.cache.get(&key).await {
                return response_from_cache(&value);
            }
            let response = self.forward(request, budget, call).await?;
            return match self.cache_value(response, policy, day_boundary, request_len) {
                Ok(value) => {
                    self.cache.insert(key, value.clone()).await;
                    response_from_cache(&value)
                }
                Err(LoadError::Uncacheable(value)) => response_from_cache(&value),
                Err(LoadError::Upstream(status)) => Err(status),
            };
        }
        let value = self
            .cache
            .try_get_with(key, async {
                let response = self
                    .forward(request, budget, call)
                    .await
                    .map_err(LoadError::Upstream)?;
                self.cache_value(response, policy, day_boundary, request_len)
            })
            .await;
        let value = match value {
            Ok(value) => value,
            Err(error) => match error.as_ref() {
                LoadError::Upstream(status) => return Err(status.clone()),
                LoadError::Uncacheable(value) => value.clone(),
            },
        };
        response_from_cache(&value)
    }

    fn cache_value<Res: Cacheable>(
        &self,
        response: Response<Res>,
        policy: Policy,
        day_boundary: Option<SystemTime>,
        request_len: usize,
    ) -> Result<Arc<Cached>, LoadError> {
        let (metadata, mut body, _) = response.into_parts();
        let now = SystemTime::now();
        let mut ttl = policy.ttl;
        if let Some(cap) = self.options.cache_ttl_cap {
            ttl = ttl.min(cap);
        }
        if let Some(boundary) = day_boundary {
            ttl = ttl.min(boundary.duration_since(now).unwrap_or_default());
        }
        if let Some(source) = body.source_metadata().as_ref()
            && let Some(expiry) = source.expires_at.and_then(|t| SystemTime::try_from(t).ok())
        {
            ttl = ttl.min(expiry.duration_since(now).unwrap_or_default());
        }
        if let Some(source) = body.source_metadata().as_mut() {
            source.expires_at = Some((now + ttl).into());
        }
        let value = Arc::new(Cached {
            body: body.into_body(),
            metadata,
            expires: Instant::now() + ttl,
        });
        let weight = value
            .body
            .encoded_len()
            .saturating_add(request_len)
            .saturating_mul(4)
            .saturating_add(2048) as u64;
        if ttl.is_zero() || weight > self.options.cache_capacity_bytes {
            Err(LoadError::Uncacheable(value))
        } else {
            Ok(value)
        }
    }
}

fn response_from_cache<Res: Cacheable>(value: &Cached) -> Result<Response<Res>, Status> {
    let body = Res::from_body(&value.body)
        .ok_or_else(|| Status::internal("cache response type mismatch"))?;
    Ok(Response::from_parts(
        value.metadata.clone(),
        body,
        Default::default(),
    ))
}

fn until_midnight(now: SystemTime) -> Duration {
    let since_epoch = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    Duration::from_secs(86400)
        - Duration::new(since_epoch.as_secs() % 86400, since_epoch.subsec_nanos())
}

const TRANSPORT_METADATA: &[&str] = &[
    "accept-encoding",
    "content-type",
    "content-length",
    "te",
    "user-agent",
    "grpc-encoding",
    "grpc-accept-encoding",
    "grpc-status",
    "grpc-message",
];
fn strip_transport_metadata(metadata: &mut MetadataMap) {
    for name in TRANSPORT_METADATA {
        metadata.remove(*name);
    }
}
fn bypass_cache(metadata: &MetadataMap, neutral: &[String]) -> bool {
    metadata.iter().any(|entry| match entry {
        KeyAndValueRef::Ascii(key, _) => {
            !TRANSPORT_METADATA.contains(&key.as_str())
                && key.as_str() != "grpc-timeout"
                && !neutral.iter().any(|name| name == key.as_str())
        }
        KeyAndValueRef::Binary(_, _) => true,
    })
}
fn request_budget(metadata: &MetadataMap, maximum: Duration) -> Result<Duration, Status> {
    let Some(timeout) = metadata.get("grpc-timeout") else {
        return Ok(maximum);
    };
    let timeout = timeout
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    if timeout.len() < 2 || timeout.len() > 9 {
        return Err(Status::invalid_argument("invalid grpc-timeout"));
    }
    let (digits, unit) = timeout.split_at(timeout.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid grpc-timeout"));
    }
    let n: u64 = digits
        .parse()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    let timeout = match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return Err(Status::invalid_argument("invalid grpc-timeout")),
    };
    Ok(timeout.min(maximum))
}

trait Cacheable: Message + Clone + Send + Sync + 'static {
    fn into_body(self) -> Body;
    fn from_body(body: &Body) -> Option<Self>;
    fn source_metadata(&mut self) -> &mut Option<pb::SourceMetadata>;
}

macro_rules! define_gateway {
    ($( $method:ident ($request:ident) -> $response:ident : $variant:ident, $policy:expr; )*) => {
        #[derive(Debug)]
        enum Body { $( $variant(pb::$response), )* }
        impl Body {
            fn encoded_len(&self) -> usize { match self { $( Self::$variant(value) => value.encoded_len(), )* } }
        }
        $( impl Cacheable for pb::$response {
            fn into_body(self) -> Body { Body::$variant(self) }
            fn from_body(body: &Body) -> Option<Self> { if let Body::$variant(value) = body { Some(value.clone()) } else { None } }
            fn source_metadata(&mut self) -> &mut Option<pb::SourceMetadata> { &mut self.metadata }
        } )*
        #[tonic::async_trait]
        impl ProxyService for Gateway {
            async fn ping(&self, request: Request<pb::PingRequest>) -> Result<Response<pb::PingResponse>, Status> {
                let budget = request_budget(request.metadata(), self.options.upstream_timeout)?;
                self.forward(request, budget, |mut client, request| async move { client.ping(request).await }).await
            }
            $( async fn $method(&self, request: Request<pb::$request>) -> Result<Response<pb::$response>, Status> {
                let policy = ($policy)(request.get_ref());
                self.cached_call(request, stringify!($method), policy, |mut client, request| async move { client.$method(request).await }).await
            } )*
        }
    }
}

define_gateway! {
    get_weather_forecast(GetWeatherForecastRequest) -> GetWeatherForecastResponse: Weather, |r: &pb::GetWeatherForecastRequest| if matches!(r.target, Some(pb::get_weather_forecast_request::Target::DayOffset(_))) { Policy::daily() } else { Policy::seconds(600) };
    get_warnings(GetWarningsRequest) -> GetWarningsResponse: Warnings, |_: &pb::GetWarningsRequest| Policy::seconds(60);
    get_observations(GetObservationsRequest) -> GetObservationsResponse: Observations, |_: &pb::GetObservationsRequest| Policy::seconds(600);
    get_sea_forecast(GetSeaForecastRequest) -> GetSeaForecastResponse: Sea, |_: &pb::GetSeaForecastRequest| Policy::daily();
    get_fire_risk(GetFireRiskRequest) -> GetFireRiskResponse: Fire, |_: &pb::GetFireRiskRequest| Policy::daily();
    get_uv_forecast(GetUvForecastRequest) -> GetUvForecastResponse: Uv, |_: &pb::GetUvForecastRequest| Policy::seconds(600);
    get_seismic_events(GetSeismicEventsRequest) -> GetSeismicEventsResponse: Seismic, |_: &pb::GetSeismicEventsRequest| Policy::seconds(600);
    get_locations(GetLocationsRequest) -> GetLocationsResponse: Locations, |_: &pb::GetLocationsRequest| Policy::seconds(86400);
    get_code_table(GetCodeTableRequest) -> GetCodeTableResponse: Codes, |_: &pb::GetCodeTableRequest| Policy::seconds(86400);
    get_geo_dataset(GetGeoDatasetRequest) -> GetGeoDatasetResponse: Geo, |r: &pb::GetGeoDatasetRequest| Policy::seconds(if r.dataset == 2 { 86400 } else { 600 });
    get_climate(GetClimateRequest) -> GetClimateResponse: Climate, |_: &pb::GetClimateRequest| Policy::seconds(3600);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn day_relative_cache_expires_at_utc_midnight() {
        let now = SystemTime::UNIX_EPOCH + Duration::new(86399, 900_000_000);
        assert_eq!(until_midnight(now), Duration::from_millis(100));
    }
}
