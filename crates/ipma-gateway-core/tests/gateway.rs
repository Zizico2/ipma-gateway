use ipma_gateway_core::{Gateway, GatewayOptions};
use ipma_proto::v1::{
    self as pb,
    proxy_service_client::ProxyServiceClient,
    proxy_service_server::{ProxyService, ProxyServiceServer},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status, metadata::MetadataMap};

#[derive(Default)]
struct State {
    calls: AtomicUsize,
    active: AtomicUsize,
    seen: Mutex<Vec<MetadataMap>>,
    delay: Duration,
    error: Option<Code>,
    source_expiry: Option<Duration>,
}
struct Active(Arc<State>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct Fake(Arc<State>);
impl Fake {
    async fn receive<T>(&self, request: &Request<T>) -> Result<Active, Status> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.seen.lock().unwrap().push(request.metadata().clone());
        self.0.active.fetch_add(1, Ordering::SeqCst);
        let active = Active(self.0.clone());
        tokio::time::sleep(self.0.delay).await;
        if let Some(code) = self.0.error {
            let mut status = Status::with_details(code, "adapter detail", vec![1, 2, 3].into());
            status
                .metadata_mut()
                .insert("x-upstream-error", "preserved".parse().unwrap());
            return Err(status);
        }
        Ok(active)
    }
    fn metadata(&self) -> pb::SourceMetadata {
        let now = SystemTime::now();
        pb::SourceMetadata {
            source_url: "https://api.ipma.pt/open-data/example.json".into(),
            fetched_at: Some(now.into()),
            expires_at: self.0.source_expiry.map(|ttl| (now + ttl).into()),
            attribution: "IPMA".into(),
            ..Default::default()
        }
    }
    fn response<T>(&self, body: T) -> Response<T> {
        let mut response = Response::new(body);
        response
            .metadata_mut()
            .insert("x-upstream", "preserved".parse().unwrap());
        response
    }
}
macro_rules! fake_service {
    ($( $method:ident($req:ident) -> $res:ident; )*) => {
        #[tonic::async_trait]
        impl ProxyService for Fake {
            async fn ping(&self, request: Request<pb::PingRequest>) -> Result<Response<pb::PingResponse>,Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::PingResponse { message: "adapter pong".into() }))
            }
            async fn get_weather_forecast(&self, request: Request<pb::GetWeatherForecastRequest>) -> Result<Response<pb::GetWeatherForecastResponse>,Status> {
                let _active = self.receive(&request).await?;
                let location_id = match request.get_ref().target { Some(pb::get_weather_forecast_request::Target::LocationId(id)) => id, _ => 0 };
                Ok(self.response(pb::GetWeatherForecastResponse { metadata: Some(self.metadata()), forecasts: vec![pb::WeatherForecast { location_id, ..Default::default() }] }))
            }
            $( async fn $method(&self, request: Request<pb::$req>) -> Result<Response<pb::$res>,Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::$res { metadata: Some(self.metadata()), ..Default::default() }))
            } )*
        }
    }
}
fake_service! {
    get_warnings(GetWarningsRequest) -> GetWarningsResponse;
    get_observations(GetObservationsRequest) -> GetObservationsResponse;
    get_sea_forecast(GetSeaForecastRequest) -> GetSeaForecastResponse;
    get_fire_risk(GetFireRiskRequest) -> GetFireRiskResponse;
    get_uv_forecast(GetUvForecastRequest) -> GetUvForecastResponse;
    get_seismic_events(GetSeismicEventsRequest) -> GetSeismicEventsResponse;
    get_locations(GetLocationsRequest) -> GetLocationsResponse;
    get_code_table(GetCodeTableRequest) -> GetCodeTableResponse;
    get_geo_dataset(GetGeoDatasetRequest) -> GetGeoDatasetResponse;
    get_climate(GetClimateRequest) -> GetClimateResponse;
}
struct Harness {
    client: ProxyServiceClient<tonic::transport::Channel>,
    state: Arc<State>,
    tasks: Vec<JoinHandle<()>>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
async fn harness(state: State, mut options: GatewayOptions) -> Harness {
    let state = Arc::new(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    options.upstream_address = format!("http://{}", listener.local_addr().unwrap());
    let fake = Fake(state.clone());
    let adapter = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ProxyServiceServer::new(fake))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let gateway = Gateway::connect(options).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ProxyServiceServer::new(gateway))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let client = ProxyServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    Harness {
        client,
        state,
        tasks: vec![adapter, task],
    }
}

#[tokio::test]
async fn caches_public_responses_and_preserves_metadata_and_fetch_time() {
    let mut h = harness(State::default(), GatewayOptions::default()).await;
    // Buf and other clients send this standard HTTP transport header.
    let request = || {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request
            .metadata_mut()
            .insert("accept-encoding", "identity".parse().unwrap());
        request
    };
    let first = h.client.get_warnings(request()).await.unwrap();
    let second = h.client.get_warnings(request()).await.unwrap();
    assert_eq!(first.get_ref(), second.get_ref());
    assert_eq!(first.metadata().get("x-upstream").unwrap(), "preserved");
    assert_eq!(second.metadata().get("x-upstream").unwrap(), "preserved");
    let metadata = second.into_inner().metadata.unwrap();
    assert_eq!(
        metadata.source_url,
        "https://api.ipma.pt/open-data/example.json"
    );
    assert_eq!(metadata.attribution, "IPMA");
    assert_eq!(
        metadata.expires_at.unwrap().seconds - metadata.fetched_at.unwrap().seconds,
        60
    );
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn coalesces_public_misses_but_keeps_request_bodies_and_methods_distinct() {
    let h = harness(
        State {
            delay: Duration::from_millis(80),
            ..Default::default()
        },
        GatewayOptions::default(),
    )
    .await;
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let mut client = h.client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .get_warnings(pb::GetWarningsRequest {})
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 1);
    let mut client = h.client.clone();
    for id in [1110600, 1010500, 1110600] {
        let response = client
            .get_weather_forecast(pb::GetWeatherForecastRequest {
                target: Some(pb::get_weather_forecast_request::Target::LocationId(id)),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.forecasts[0].location_id, id);
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 3);
    client
        .get_observations(pb::GetObservationsRequest {})
        .await
        .unwrap();
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn application_metadata_bypasses_cache_while_deadlines_reuse_public_hits() {
    let mut h = harness(State::default(), GatewayOptions::default()).await;
    h.client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap();
    h.client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap();
    for name in ["authorization", "x-tenant", "x-request-id"] {
        for _ in 0..2 {
            let mut request = Request::new(pb::GetWarningsRequest {});
            request
                .metadata_mut()
                .insert(name, "synthetic-test-value".parse().unwrap());
            let result = h.client.get_warnings(request).await.unwrap();
            assert!(result.into_inner().metadata.unwrap().expires_at.is_none());
            assert_eq!(
                h.state
                    .seen
                    .lock()
                    .unwrap()
                    .last()
                    .unwrap()
                    .get(name)
                    .unwrap(),
                "synthetic-test-value"
            );
        }
    }
    for _ in 0..2 {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request.set_timeout(Duration::from_secs(1));
        h.client.get_warnings(request).await.unwrap();
    }
    for _ in 0..2 {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request.metadata_mut().insert_bin(
            "x-context-bin",
            tonic::metadata::MetadataValue::from_bytes(&[0, 1, 255]),
        );
        h.client.get_warnings(request).await.unwrap();
        assert_eq!(
            h.state
                .seen
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .get_bin("x-context-bin")
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            &[0, 1, 255]
        );
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 9);
}

#[tokio::test]
async fn expired_entries_refetch_and_upstream_expiry_is_respected() {
    let mut h = harness(
        State {
            source_expiry: Some(Duration::from_millis(40)),
            ..Default::default()
        },
        GatewayOptions::default(),
    )
    .await;
    let first = h
        .client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap()
        .into_inner();
    tokio::time::sleep(Duration::from_millis(70)).await;
    let second = h
        .client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_ne!(
        first.metadata.unwrap().fetched_at,
        second.metadata.unwrap().fetched_at
    );
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 2);
    let mut h = harness(
        State::default(),
        GatewayOptions {
            cache_ttl_cap: Some(Duration::from_millis(40)),
            ..Default::default()
        },
    )
    .await;
    h.client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(70)).await;
    h.client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap();
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn oversized_entries_are_served_without_admission() {
    let mut h = harness(
        State::default(),
        GatewayOptions {
            cache_capacity_bytes: 1,
            ..Default::default()
        },
    )
    .await;
    for _ in 0..2 {
        h.client
            .get_warnings(pb::GetWarningsRequest {})
            .await
            .unwrap();
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn errors_are_preserved_and_never_cached() {
    for code in [
        Code::InvalidArgument,
        Code::NotFound,
        Code::Unavailable,
        Code::ResourceExhausted,
        Code::DataLoss,
    ] {
        let mut h = harness(
            State {
                error: Some(code),
                ..Default::default()
            },
            GatewayOptions::default(),
        )
        .await;
        for _ in 0..2 {
            let status = h
                .client
                .get_warnings(pb::GetWarningsRequest {})
                .await
                .unwrap_err();
            assert_eq!(status.code(), code);
            assert_eq!(status.message(), "adapter detail");
            assert_eq!(status.details(), &[1, 2, 3]);
            assert_eq!(
                status.metadata().get("x-upstream-error").unwrap(),
                "preserved"
            );
        }
        assert_eq!(h.state.calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn short_client_deadlines_and_gateway_timeouts_are_enforced() {
    let mut h = harness(
        State {
            delay: Duration::from_millis(150),
            ..Default::default()
        },
        GatewayOptions::default(),
    )
    .await;
    let mut request = Request::new(pb::GetWarningsRequest {});
    request.set_timeout(Duration::from_millis(25));
    let status = h.client.get_warnings(request).await.unwrap_err();
    assert!(matches!(
        status.code(),
        Code::DeadlineExceeded | Code::Cancelled
    ));
    let mut h = harness(
        State {
            delay: Duration::from_millis(150),
            ..Default::default()
        },
        GatewayOptions {
            upstream_timeout: Duration::from_millis(25),
            ..Default::default()
        },
    )
    .await;
    // Tonic 0.14 reports transport deadline expiry as CANCELLED; the outer
    // Tokio deadline reports DEADLINE_EXCEEDED. Both terminate the slow call.
    let status = h
        .client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap_err();
    assert!(matches!(
        status.code(),
        Code::DeadlineExceeded | Code::Cancelled
    ));
}

#[tokio::test]
async fn ping_is_forwarded_each_time_and_all_data_methods_delegate() {
    let mut h = harness(State::default(), GatewayOptions::default()).await;
    for _ in 0..2 {
        assert_eq!(
            h.client
                .ping(pb::PingRequest {})
                .await
                .unwrap()
                .into_inner()
                .message,
            "adapter pong"
        );
    }
    h.client
        .get_weather_forecast(pb::GetWeatherForecastRequest { target: None })
        .await
        .unwrap();
    h.client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap();
    h.client
        .get_observations(pb::GetObservationsRequest {})
        .await
        .unwrap();
    h.client
        .get_sea_forecast(pb::GetSeaForecastRequest {
            day_offset: Some(0),
        })
        .await
        .unwrap();
    h.client
        .get_fire_risk(pb::GetFireRiskRequest {
            day_offset: Some(0),
        })
        .await
        .unwrap();
    h.client
        .get_uv_forecast(pb::GetUvForecastRequest {})
        .await
        .unwrap();
    h.client
        .get_seismic_events(pb::GetSeismicEventsRequest { region: 1 })
        .await
        .unwrap();
    h.client
        .get_locations(pb::GetLocationsRequest { kind: 1 })
        .await
        .unwrap();
    h.client
        .get_code_table(pb::GetCodeTableRequest { kind: 1 })
        .await
        .unwrap();
    h.client
        .get_geo_dataset(pb::GetGeoDatasetRequest { dataset: 1 })
        .await
        .unwrap();
    h.client
        .get_climate(pb::GetClimateRequest { dataset: 1 })
        .await
        .unwrap();
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 13);
}

#[tokio::test]
async fn saturation_is_bounded_and_cancellation_releases_forward_capacity() {
    let h = harness(
        State {
            delay: Duration::from_millis(150),
            ..Default::default()
        },
        GatewayOptions {
            max_concurrent_forwards: 1,
            ..Default::default()
        },
    )
    .await;
    let mut client = h.client.clone();
    let task = tokio::spawn(async move {
        let mut req = Request::new(pb::GetWarningsRequest {});
        req.metadata_mut()
            .insert("x-bypass", "yes".parse().unwrap());
        client.get_warnings(req).await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.state.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut client = h.client.clone();
    assert_eq!(
        client
            .get_uv_forecast(pb::GetUvForecastRequest {})
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    task.abort();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match client.get_uv_forecast(pb::GetUvForecastRequest {}).await {
                Ok(_) => break,
                Err(status) if status.code() == Code::ResourceExhausted => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(status) => panic!("unexpected status: {status}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn one_callers_short_deadline_cannot_poison_another_callers_result() {
    let h = harness(
        State {
            delay: Duration::from_millis(80),
            ..Default::default()
        },
        GatewayOptions::default(),
    )
    .await;
    let mut short = h.client.clone();
    let first = tokio::spawn(async move {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request.set_timeout(Duration::from_millis(10));
        short.get_warnings(request).await
    });
    let mut long = h.client.clone();
    let second = tokio::spawn(async move {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request.set_timeout(Duration::from_secs(1));
        long.get_warnings(request).await
    });
    let code = first.await.unwrap().unwrap_err().code();
    assert!(matches!(code, Code::DeadlineExceeded | Code::Cancelled));
    second.await.unwrap().unwrap();
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelling_a_public_cache_miss_does_not_stick_the_key() {
    let h = harness(
        State {
            delay: Duration::from_millis(80),
            ..Default::default()
        },
        GatewayOptions {
            max_concurrent_forwards: 1,
            ..Default::default()
        },
    )
    .await;
    let mut client = h.client.clone();
    let task = tokio::spawn(async move { client.get_warnings(pb::GetWarningsRequest {}).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while h.state.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let mut client = h.client.clone();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match client.get_warnings(pb::GetWarningsRequest {}).await {
                Ok(_) => break,
                Err(status) if status.code() == Code::ResourceExhausted => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(status) => panic!("unexpected status: {status}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deadline_bearing_public_misses_can_populate_the_cache() {
    let mut h = harness(State::default(), GatewayOptions::default()).await;
    let mut fetched_at = None;
    for _ in 0..2 {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request.set_timeout(Duration::from_secs(1));
        let metadata = h
            .client
            .get_warnings(request)
            .await
            .unwrap()
            .into_inner()
            .metadata
            .unwrap();
        assert!(metadata.expires_at.is_some());
        if let Some(first) = fetched_at {
            assert_eq!(metadata.fetched_at, Some(first));
        }
        fetched_at = metadata.fetched_at;
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 1);
    assert!(h.state.seen.lock().unwrap()[0].contains_key("grpc-timeout"));
}
