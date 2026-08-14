# fingerjoin

WebFinger aggregation proxy for Kubernetes.

Host several fediverse (ActivityPub) applications on one domain: fingerjoin serves
`/.well-known/webfinger` at the domain apex, fans each query out to every application
in the cluster that can answer it, and merges the results into a single JRD response.

## Why

WebFinger ([RFC 7033](https://www.rfc-editor.org/rfc/rfc7033)) is served from exactly
one place per domain: `https://example.com/.well-known/webfinger`. As soon as two
applications on the same domain want to answer WebFinger — say Mastodon and a key
server — they conflict. fingerjoin sits at that path and lets each application answer
for its own resources, with no per-application configuration beyond an annotation.

## How it works

- Watches Gateway API `HTTPRoute` resources cluster-wide, polling every 30 seconds.
- Any route annotated with `fingerjoin.naktis.eu/webfinger: "true"` is registered as a
  backend. The backend service is taken from the route's `backendRefs` (an explicit
  `namespace` on the ref is honored; non-`Service` refs are skipped) and resolved to a
  namespace-qualified in-cluster FQDN (`<service>.<namespace>.svc.<cluster domain>`).
- Each incoming WebFinger request is forwarded to all backends concurrently (at most 10
  in flight, 5 second timeout each, response bodies capped at 256 KiB, redirects not
  followed). `rel` query parameters are forwarded too.
- Successful JRD responses are merged in priority order (higher `priority` annotation
  wins): the first subject wins, properties keep the first value per key, aliases are
  combined and deduplicated, and links are deduplicated by (`rel`, `href`). Nonstandard
  JRD members (such as the `template` on Mastodon's subscribe link) pass through
  unchanged. If the query carried `rel` parameters, the merged links are filtered
  accordingly (RFC 7033 §4.3).
- Responses carry `Access-Control-Allow-Origin: *`, as required for browser-based
  clients (RFC 7033 §5).

### HTTPRoute annotations

| Annotation | Default | Meaning |
|---|---|---|
| `fingerjoin.naktis.eu/webfinger` | — | `"true"` registers this route's backend |
| `fingerjoin.naktis.eu/https` | `"false"` | Talk to the backend over HTTPS (default port 443) |
| `fingerjoin.naktis.eu/backend` | `"0"` | Index of the route rule whose first `backendRef` is used |
| `fingerjoin.naktis.eu/priority` | `"50"` | Merge priority; on conflicts (same subject, property, or link key) the backend with the **higher** value wins |

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: mastodon
  annotations:
    fingerjoin.naktis.eu/webfinger: "true"
spec:
  parentRefs:
    - name: my-gateway
  rules:
    - backendRefs:
        - name: mastodon-web
          port: 3000
```

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `CLUSTER_DOMAIN` | `cluster.local` | Cluster DNS domain used to build backend FQDNs |
| `PORT` | `8080` | HTTP listen port |
| `RUST_LOG` | `info` | [tracing filter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html), e.g. `info,fingerjoin=debug` |
| `LOG_FORMAT` | text | Set to `json` for structured JSON logs |
| `EXTRA_CA_CERTS` | — | Path to a PEM bundle of additional CA certificates to trust for HTTPS backends (e.g. an in-cluster CA) |

## Endpoints

| Path | Response |
|---|---|
| `GET /.well-known/webfinger?resource=<uri>[&rel=...]` | Merged JRD as `application/jrd+json`. The resource may be any URI (`acct:`, `https:`, ...). |
| `GET /healthz` (alias `/health`) | Liveness: always `200` with backend count and seconds since the last HTTPRoute sync |
| `GET /readyz` | Readiness: `200` once the first HTTPRoute sync completed, `503` before |

WebFinger status codes follow federation semantics — a definitive negative answer is
only given when every backend answered:

| Status | Meaning |
|---|---|
| `200` | At least one backend returned a JRD |
| `400` | `resource` parameter missing or not a URI |
| `404` | Every backend answered, none knows the resource (backend 404s and 4xx declines both count) |
| `410` | Every backend answered, at least one reports the resource permanently gone |
| `502` | At least one backend failed (timeout, 5xx, bad JRD) and none succeeded — retryable, never cached as "no such user" |
| `503` | No backends registered yet (`Retry-After` set) |

## Deployment

A Helm chart (built on the [bjw-s common library](https://github.com/bjw-s-labs/helm-charts))
is included:

```sh
helm install fingerjoin oci://ghcr.io/m00nwtchr/charts/fingerjoin -n fingerjoin --create-namespace
```

fingerjoin needs `get`/`list`/`watch` on `httproutes.gateway.networking.k8s.io`
cluster-wide; the chart ships the ClusterRole and binding.

## Development

```sh
cargo test
cargo clippy --all-targets
```

A [devenv](https://devenv.sh) shell with the toolchain and pre-commit hooks is provided
(`devenv shell`).

## License

[MPL-2.0](LICENSE)
