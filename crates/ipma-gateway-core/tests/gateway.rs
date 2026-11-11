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
                Ok(self.response(pb::GetWeatherForecastResponse { metadata: Some(self.metadata()), forecasts: vec![pb::WeatherForecast {
                    location_id,
                    weather_type_code: Some(1234),
                    weather_type: Some(pb::WeatherTypeValue { value: 777, source_code: 1234 }),
                    wind_speed_class: Some(2),
                    wind_speed_classification: Some(pb::WindSpeedClassValue { value: pb::WindSpeedClass::Moderate as i32, source_code: 2 }),
                    precipitation_intensity_class: Some(0),
                    precipitation_intensity: Some(pb::PrecipitationIntensityValue { value: pb::PrecipitationIntensity::NoPrecipitation as i32, source_code: 0 }),
                    prevailing_wind_direction: "N".into(),
                    wind_direction: Some(pb::DirectionValue { value: pb::CompassDirection::North as i32, source: Some(pb::direction_value::Source::Text("N".into())) }),
                    ..Default::default()
                }] }))
            }
            async fn get_warnings(&self, request: Request<pb::GetWarningsRequest>) -> Result<Response<pb::GetWarningsResponse>, Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::GetWarningsResponse { metadata: Some(self.metadata()), warnings: vec![pb::Warning {
                    level: "purple".into(), type_name: "Future category".into(),
                    warning_level: Some(pb::WarningLevelValue { value: pb::WarningLevel::Other as i32, source_value: "purple".into() }),
                    warning_type: Some(pb::WarningTypeValue { value: pb::WarningType::Other as i32, source_value: "Future category".into() }),
                    ..Default::default()
                }] }))
            }
            async fn get_fire_risk(&self, request: Request<pb::GetFireRiskRequest>) -> Result<Response<pb::GetFireRiskResponse>, Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::GetFireRiskResponse { metadata: Some(self.metadata()), risks: vec![pb::FireRisk {
                    municipality_id: "0101".into(), risk_class: Some(5),
                    classification: Some(pb::FireRiskClassValue { value: pb::FireRiskClass::Maximum as i32, source_code: 5 }),
                    ..Default::default()
                }], ..Default::default() }))
            }
            async fn get_locations(&self, request: Request<pb::GetLocationsRequest>) -> Result<Response<pb::GetLocationsResponse>, Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::GetLocationsResponse {
                    metadata: Some(self.metadata()),
                    locations: vec![pb::Location {
                        location_id: 9999999,
                        location: Some(pb::ForecastLocationValue { value: 777, source_code: 9999999 }),
                        municipality: Some(pb::MunicipalityValue {
                            value: pb::Municipality::Other as i32, source_code: "9901".into(),
                            source_local_code: Some(1), district: Some(pb::DistrictValue { value: 888, source_code: 99 }),
                        }),
                        ..Default::default()
                    }],
                }))
            }
            async fn get_geo_dataset(&self, request: Request<pb::GetGeoDatasetRequest>) -> Result<Response<pb::GetGeoDatasetResponse>, Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::GetGeoDatasetResponse {
                    metadata: Some(self.metadata()), geobuf: vec![0, 127, 255].into(),
                    feature_categories: vec![pb::GeoFeatureCategories {
                        feature_index: 7,
                        categories: Some(pb::geo_feature_categories::Categories::BivalveZone(pb::BivalveZoneCategories {
                            zone: Some(pb::BivalveZoneValue { value: 999, source_value: "FUTURE".into() }),
                            status: Some(pb::BivalveZoneStatusValue { value: pb::BivalveZoneStatus::Close as i32, source_value: "CLOSE".into() }),
                            species_permissions: vec![pb::BivalveSpeciesPermission {
                                species: Some(pb::BivalveSpeciesValue { value: 777, source_value: "Future taxon".into(), common_name: "Future label".into() }),
                                sanitary_class: Some(pb::BivalveSanitaryClassValue { value: pb::BivalveSanitaryClass::BStar as i32, source_value: "B*".into() }),
                                permission: Some(pb::BivalveHarvestPermissionValue { value: 888, source_value: "pending".into() }),
                            }],
                            ..Default::default()
                        })),
                    }],
                }))
            }
            async fn get_code_table(&self, request: Request<pb::GetCodeTableRequest>) -> Result<Response<pb::GetCodeTableResponse>, Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::GetCodeTableResponse { metadata: Some(self.metadata()), entries: vec![pb::CodeEntry {
                    code: 1234, description_portuguese: "Nova classe".into(), description_english: "New class".into(),
                    classification: Some(pb::code_entry::Classification::WeatherType(888)),
                }] }))
            }
            $( async fn $method(&self, request: Request<pb::$req>) -> Result<Response<pb::$res>,Status> {
                let _active = self.receive(&request).await?;
                Ok(self.response(pb::$res { metadata: Some(self.metadata()), ..Default::default() }))
            } )*
        }
    }
}
fake_service! {
    get_observations(GetObservationsRequest) -> GetObservationsResponse;
    get_sea_forecast(GetSeaForecastRequest) -> GetSeaForecastResponse;
    get_uv_forecast(GetUvForecastRequest) -> GetUvForecastResponse;
    get_seismic_events(GetSeismicEventsRequest) -> GetSeismicEventsResponse;
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

#[tokio::test]
async fn categorical_values_and_future_enum_numbers_survive_forwarding_and_cache() {
    let mut h = harness(State::default(), GatewayOptions::default()).await;
    let weather = pb::GetWeatherForecastRequest {
        target: Some(pb::get_weather_forecast_request::Target::LocationId(
            1110600,
        )),
    };
    let first = h
        .client
        .get_weather_forecast(weather)
        .await
        .unwrap()
        .into_inner();
    let second = h
        .client
        .get_weather_forecast(weather)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, second);
    let forecast = &second.forecasts[0];
    assert_eq!(forecast.weather_type.as_ref().unwrap().value, 777);
    assert_eq!(forecast.weather_type.as_ref().unwrap().source_code, 1234);
    assert_eq!(
        forecast.wind_speed_classification.as_ref().unwrap().value(),
        pb::WindSpeedClass::Moderate
    );
    assert_eq!(
        forecast.precipitation_intensity.as_ref().unwrap().value(),
        pb::PrecipitationIntensity::NoPrecipitation
    );
    assert_eq!(
        forecast
            .precipitation_intensity
            .as_ref()
            .unwrap()
            .source_code,
        0
    );
    assert_eq!(
        forecast.wind_direction.as_ref().unwrap().source,
        Some(pb::direction_value::Source::Text("N".into()))
    );
    let first = h
        .client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap()
        .into_inner();
    let second = h
        .client
        .get_warnings(pb::GetWarningsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, second);
    assert_eq!(
        second.warnings[0].warning_level.as_ref().unwrap().value(),
        pb::WarningLevel::Other
    );
    assert_eq!(
        second.warnings[0]
            .warning_level
            .as_ref()
            .unwrap()
            .source_value,
        "purple"
    );
    assert_eq!(
        second.warnings[0]
            .warning_type
            .as_ref()
            .unwrap()
            .source_value,
        "Future category"
    );
    let first = h
        .client
        .get_fire_risk(pb::GetFireRiskRequest {
            day_offset: Some(0),
        })
        .await
        .unwrap()
        .into_inner();
    let second = h
        .client
        .get_fire_risk(pb::GetFireRiskRequest {
            day_offset: Some(0),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, second);
    assert_eq!(
        second.risks[0].classification.as_ref().unwrap().value(),
        pb::FireRiskClass::Maximum
    );
    assert_eq!(second.risks[0].municipality_id, "0101");
    let first = h
        .client
        .get_code_table(pb::GetCodeTableRequest { kind: 1 })
        .await
        .unwrap()
        .into_inner();
    let second = h
        .client
        .get_code_table(pb::GetCodeTableRequest { kind: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, second);
    assert_eq!(
        second.entries[0].classification,
        Some(pb::code_entry::Classification::WeatherType(888))
    );
    assert_eq!(second.entries[0].description_portuguese, "Nova classe");
    assert_eq!(second.entries[0].code, 1234);
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn reference_composition_and_geobuf_companions_survive_cache_hits() {
    let mut h = harness(State::default(), GatewayOptions::default()).await;
    let first = h
        .client
        .get_locations(pb::GetLocationsRequest { kind: 1 })
        .await
        .unwrap()
        .into_inner();
    let cached = h
        .client
        .get_locations(pb::GetLocationsRequest { kind: 1 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, cached);
    let location = &cached.locations[0];
    assert_eq!(location.location.as_ref().unwrap().value, 777);
    let municipality = location.municipality.as_ref().unwrap();
    assert_eq!(municipality.source_code, "9901");
    assert_eq!(municipality.source_local_code, Some(1));
    assert_eq!(municipality.district.as_ref().unwrap().value, 888);
    let first = h
        .client
        .get_geo_dataset(pb::GetGeoDatasetRequest { dataset: 3 })
        .await
        .unwrap()
        .into_inner();
    let cached = h
        .client
        .get_geo_dataset(pb::GetGeoDatasetRequest { dataset: 3 })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, cached);
    assert_eq!(cached.geobuf.as_ref(), &[0, 127, 255]);
    assert_eq!(cached.feature_categories[0].feature_index, 7);
    let Some(pb::geo_feature_categories::Categories::BivalveZone(zone)) =
        &cached.feature_categories[0].categories
    else {
        panic!("lost feature categories");
    };
    assert_eq!(zone.zone.as_ref().unwrap().value, 999);
    assert_eq!(zone.zone.as_ref().unwrap().source_value, "FUTURE");
    assert_eq!(
        zone.status.as_ref().unwrap().value(),
        pb::BivalveZoneStatus::Close
    );
    let species = &zone.species_permissions[0];
    assert_eq!(species.species.as_ref().unwrap().value, 777);
    assert_eq!(
        species.species.as_ref().unwrap().common_name,
        "Future label"
    );
    assert_eq!(species.permission.as_ref().unwrap().value, 888);
    assert_eq!(species.permission.as_ref().unwrap().source_value, "pending");
    assert_eq!(species.sanitary_class.as_ref().unwrap().source_value, "B*");
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn trusted_ingress_metadata_can_share_public_cache_without_being_dropped() {
    let mut h = harness(
        State::default(),
        GatewayOptions {
            cache_neutral_metadata: vec!["x-forwarded-for".into(), "x-trace-id".into()],
            ..Default::default()
        },
    )
    .await;
    for value in ["192.0.2.1", "192.0.2.2"] {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request
            .metadata_mut()
            .insert("x-forwarded-for", value.parse().unwrap());
        request
            .metadata_mut()
            .insert("x-trace-id", value.parse().unwrap());
        assert!(
            h.client
                .get_warnings(request)
                .await
                .unwrap()
                .into_inner()
                .metadata
                .unwrap()
                .expires_at
                .is_some()
        );
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.state.seen.lock().unwrap()[0]
            .get("x-forwarded-for")
            .unwrap(),
        "192.0.2.1"
    );
    for _ in 0..2 {
        let mut request = Request::new(pb::GetWarningsRequest {});
        request
            .metadata_mut()
            .insert("authorization", "Bearer synthetic".parse().unwrap());
        request
            .metadata_mut()
            .insert("x-forwarded-for", "192.0.2.1".parse().unwrap());
        assert!(
            h.client
                .get_warnings(request)
                .await
                .unwrap()
                .into_inner()
                .metadata
                .unwrap()
                .expires_at
                .is_none()
        );
    }
    assert_eq!(h.state.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn cache_neutral_configuration_rejects_credentials_tenant_and_binary_context() {
    for name in [
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "x-tenant",
        "x-tenant-id",
        "tenant-id",
        "x-context-bin",
        "Uppercase",
        "",
        "bad name",
    ] {
        let options = GatewayOptions {
            cache_neutral_metadata: vec![name.into()],
            ..Default::default()
        };
        let result = Gateway::connect(options).await;
        assert!(result.is_err(), "accepted {name}");
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("cache-neutral metadata")
        );
    }
}
