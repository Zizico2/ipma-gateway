# ipma-gateway

Policy service over [ipma-proxy](https://github.com/Zizico2/ipma-proxy).
Implements the same `ipma.proxy.v1.ProxyService` using generated bindings from
[ipma-proto](https://github.com/Zizico2/ipma-proto), consumed through a full-SHA
pinned Cargo Git dependency. All data comes from the adapter over gRPC.

```text
client -> ipma-gateway :50052 -> ipma-proxy :50051 -> IPMA HTTPS
```

The initial add-on policy is caching. Authentication, authorization, rate
limiting and quotas belong in this repository and can be added as application
requirements are defined. They are not currently enabled or implemented.
Use the adapter directly for one-to-one upstream requests.

`crates/ipma-gateway-core` implements delegation and cache policy;
`crates/ipma-gateway` owns CLI/env configuration, runtime, health, reflection
and graceful shutdown. The gateway does not import the adapter implementation
or perform HTTP/GeoJSON/CSV conversion. It shares only the versioned API crate.

Start the adapter first, then run `just run` here. The default gateway listens
on `127.0.0.1:50052` and connects to `http://127.0.0.1:50051`.
`just run --help` lists CLI options. Optional `.env` is loaded; existing
environment variables override it, and CLI arguments take precedence.

| Environment variable | Default |
| --- | --- |
| `IPMA_GATEWAY_LISTEN_ADDR` | `127.0.0.1:50052` |
| `IPMA_GATEWAY_UPSTREAM_ADDRESS` | `http://127.0.0.1:50051` |
| `IPMA_GATEWAY_UPSTREAM_TIMEOUT_SECONDS` | `15` |
| `IPMA_GATEWAY_CACHE_MIB` | `64` |
| `IPMA_GATEWAY_MAX_CONCURRENT_FORWARDS` | `8` |
| `IPMA_GATEWAY_CACHE_TTL_CAP_SECONDS` | Unset |
| `RUST_LOG` | `ipma_gateway=info,ipma_gateway_core=info` |

The upstream must be an HTTP(S) origin without credentials/path/query. HTTPS
uses native certificate roots. Set TLS and authentication at your ingress
when serving beyond a trusted network. This is a gRPC-only service.

Caching uses the RPC method and the complete protobuf request as the key.
Concurrent eligible misses are coalesced. Failures are never cached, expired
entries are refetched, and failures do not trigger stale-response fallback.
Entries larger than the configured budget are served without admission.
Cache weights estimate heap usage at four times encoded request/response size
plus 2 KiB; this is a best-effort cache budget, not an RSS limit.

| Dataset | Maximum cache lifetime |
| --- | --- |
| Warnings | 60 seconds |
| Location/code tables and station catalogue | 24 hours |
| Climate statistics | 1 hour |
| Other datasets | 10 minutes |

Day-relative requests expire by the midnight UTC following request start.
A fetch crossing that boundary is served without caching. Upstream expiry,
when present, further shortens retention; `--cache-ttl-cap-seconds` can shorten
all lifetimes. The gateway sets `SourceMetadata.expires_at` to its effective
cache expiry and preserves the adapter's fetch time, source update time,
source URL, attribution and all data/Geobuf bytes.

Requests with any application metadata (including authorization, tenant IDs,
binary context and request IDs) bypass the shared cache. This conservative
default keeps caller context isolated from public entries. Public requests
with explicit deadlines can reuse cache hits; their misses forward separately
with their individual budgets and may populate the cache. They are never
coalesced under another caller's shorter deadline. Public misses without an
explicit deadline are coalesced under the configured upstream timeout.
Standard transport headers do not vary cache keys. `Ping` always delegates.

Request metadata is forwarded after transport headers are rebuilt. Custom
response metadata and upstream status code/message/details/error metadata are
preserved. Forwarding is bounded by the smaller of the caller deadline and
configured timeout. Tonic can report transport deadline expiry as CANCELLED;
the gateway's outer timeout reports DEADLINE_EXCEEDED. Dropping a request
drops its forwarding future; cache initializers are cancellation-safe.
The shared connection pool and semaphore bound outbound work. Saturation
returns RESOURCE_EXHAUSTED. Requests are limited to 64 KiB, responses to 32 MiB;
gzip is available through negotiation. Health and reflection describe this
process; `just ping` checks the full gateway-to-adapter route.

`just verify` runs formatting, Clippy with warnings denied and Rust tests.
Tests use actual local gRPC servers to cover caching/coalescing, distinct
requests, expiry/caps, metadata/context isolation, error details, all RPCs,
deadlines, cancellation and capacity. `just release` builds an optimized
binary. `just smoke` exercises all 26 selections against a running gateway;
its second argument optionally adds forecast calls, e.g.
`just smoke http://127.0.0.1:50052 100` for a warm-cache check.

Schemas and code generation are maintained in ipma-proto. Update the pinned
`rev` in Cargo.toml and refresh Cargo.lock when adopting an API revision.
The repository builds independently of sibling checkouts; no local patches
or path overrides are required.
