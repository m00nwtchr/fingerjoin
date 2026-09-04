# Immediate Service Sync and Kubernetes Watch

## Purpose

Remove the startup and change-detection latency caused by the current 30-second
polling reconciler. fingerjoin should perform its first complete Service sync as
soon as the Kubernetes watcher initializes and should publish Service changes as
the Kubernetes API reports them.

## Scope

This change covers the Service reconciler and the retry hint used when no
backends are available:

- enable kube-rs runtime watcher support;
- replace the periodic full-list loop with a resilient Service watcher;
- perform and publish the initial complete Service snapshot immediately;
- apply Service additions, updates, deletions, and full relists to backend state;
- preserve the last known backend snapshot during transient watcher failures;
- replace the obsolete polling interval constant with a ten-second retry hint;
- add deterministic unit coverage for watcher-event state transitions.

No chart, CI, HTTP route, backend-fetch, or Service-to-backend mapping contract
changes are part of this design.

## Decisions

1. Use `kube::runtime::watcher` rather than raw `Api::watch`. The kube runtime
   owns watch reconnection, resource-version expiry handling, and backoff.
2. Use `watcher::Config::default()` and
   `WatchStreamExt::default_backoff()`.
3. Remove the 30-second periodic poll. Watcher initialization and relists are
   the authoritative full-list boundaries.
4. Keep the reconciler's Service cache local to its single task in a
   `HashMap` keyed by namespace/name. No concurrent map or new shared state API
   is needed.
5. Keep the last successful backend snapshot and readiness state after a
   watcher error. A complete `Init`/`InitApply`/`InitDone` cycle replaces the
   complete cache, removing stale Services after recovery.
6. Use a ten-second `Retry-After` hint for `503 NoBackends` responses. This is a
   client retry suggestion aligned with the watcher's initial recovery backoff,
   not a promise that a backend will appear within ten seconds.

## Architecture and data flow

`main` continues to construct `BackendState`, spawn `start_reconciler`, and
start serving HTTP independently. `start_reconciler` creates
`Api<Service>::all(client)` and a kube runtime watcher stream. The reconciler
processes stream events serially, so cache mutation and backend publication
cannot race each other.

The event handling rules are:

- `Event::Init`: start a pending Service snapshot without changing the live
  cache.
- `Event::InitApply(service)`: insert the Service into the pending snapshot.
- `Event::InitDone`: swap the pending snapshot into the live cache, derive one
  complete sorted/deduplicated backend vector through the existing
  `backends_from_services` function, and call `BackendState::update`.
- `Event::Apply(service)`: insert or replace the Service in the live cache,
  derive the current backend vector, and publish it.
- `Event::Delete(service)`: remove the Service from the live cache, derive the
  current backend vector, and publish it.

The watcher's initial list is represented by an `Init` sequence ending at
`InitDone`; buffering until `InitDone` prevents readiness from exposing a
partial snapshot. The same sequence is used for relists after resource-version
expiry, so stale Services are removed atomically. Subsequent applied/deleted
events update the derived state without waiting for the old polling interval.

## Failure and readiness behavior

Watcher errors are logged and do not modify `BackendState`. The watcher stream's
default backoff and reconnect behavior retries the API operation and produces an
`Init` sequence when it relists. The reconciler keeps consuming the stream, so an
individual watch failure cannot silently stop backend updates.

Before the first successful complete snapshot, readiness remains `503` and the
backend list remains empty. After one successful snapshot, readiness remains
`200` and requests continue using the last known backend list during transient
watch/API failures. A later full relist atomically replaces the derived list
and refreshes the sync timestamp.

## Testing

The existing pure Service mapping tests remain unchanged. New deterministic
tests will exercise the event-cache reducer without a live Kubernetes cluster:

- an initial `Init`/`InitApply`/`InitDone` sequence publishes all current
  Services only at `InitDone`;
- an applied event adds a Service;
- an applied event replaces an existing Service;
- a deleted event removes a Service;
- a relist sequence removes stale cached Services at `InitDone`;
- derived backend ordering and priority behavior remain intact;
- an incomplete initialization sequence does not publish partial state.

Validation will include:

- `cargo test --all-targets --all-features`;
- `cargo clippy --all-targets --all-features -- -D warnings`;
- `cargo fmt --all -- --check`;
- `git diff --check`.

## Acceptance criteria

1. A newly started process performs its first successful Service list without
   waiting for 30 seconds.
2. A Service create, update, or delete is reflected after its watch event and
   does not wait for a periodic poll.
3. A reconnect or resource-version-expiry relist removes stale entries and
   produces the same complete derived state as a fresh list.
4. Transient watcher failures do not clear the last known backends or readiness.
5. No periodic full-list ticker remains in the reconciler.
6. Existing application behavior and all validation commands remain green.

## Alternatives rejected

- **Keep polling alongside a watch:** duplicates API load and leaves two state
  update paths with avoidable ordering complexity.
- **Use a reflector store:** idiomatic for larger controllers, but unnecessary
  additional lifecycle and abstraction for one watched resource type.
- **Use raw `Api::watch`:** requires reimplementing reconnect, backoff,
  resource-version expiry, and relist behavior that kube-rs already provides.
