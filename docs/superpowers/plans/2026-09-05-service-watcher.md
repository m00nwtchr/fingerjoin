# Immediate Service Sync and Kubernetes Watch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace fingerjoin's delayed 30-second Service polling loop with an immediate initial synchronization and resilient event-driven Service watch.

**Architecture:** Enable kube-rs runtime support and consume `kube::runtime::watcher` in the existing reconciler task. A local namespace/name-keyed `HashMap` buffers `Init`/`InitApply` events until `InitDone`, then applies `Apply` and `Delete` events incrementally; each complete state is converted through the existing `backends_from_services` function and published to `BackendState`.

**Tech Stack:** Rust 2021, Tokio, kube 3.1.0 / kube-runtime, futures, k8s-openapi, Cargo unit tests.

**Spec:** `docs/superpowers/specs/2026-09-05-service-watcher-design.md`

## Global Constraints

- Use `kube::runtime::watcher` rather than raw `Api::watch`.
- Use `watcher::Config::default()` and `WatchStreamExt::default_backoff()`.
- Remove the 30-second periodic poll; watcher initialization and relists are the full-list boundaries.
- Keep the reconciler Service cache local to its single task in a `HashMap` keyed by namespace/name.
- Keep the last successful backend snapshot and readiness state after watcher errors.
- Use a ten-second `Retry-After` hint for `503 NoBackends` responses.
- Buffer `Init`/`InitApply` events and publish only at `InitDone`; use `Apply` and `Delete` for steady-state changes.
- Do not change chart, CI, HTTP route, backend-fetch, or Service-to-backend mapping contracts.
- Keep the existing pure Service mapping tests and add deterministic event-cache tests without a live Kubernetes cluster.

---

### Task 1: Add and test the Service event cache

**Files:**
- Modify: `Cargo.toml:12` — add kube's `runtime` feature alongside `client`.
- Modify: `Cargo.lock` — regenerate only the dependency closure caused by enabling kube runtime.
- Modify: `src/k8s.rs` — add the local cache reducer and its unit tests.

**Interfaces:**
- Produces `ServiceStore::apply(&mut self, event: kube::runtime::watcher::Event<Service>) -> Option<Vec<Service>>`.
- Produces `service_key(service: &Service) -> String`, using `<namespace>/<name>` with `default` for a missing namespace.
- `None` from `ServiceStore::apply` means an initialization snapshot is incomplete and must not be published.
- `Some(Vec<Service>)` is the complete current snapshot to pass to `backends_from_services`.

- [ ] **Step 1: Enable the kube runtime feature**

Change the dependency declaration to:

```toml
kube = { version = "3", features = ["client", "runtime"] }
```

Run:

```bash
cargo check
```

Expected: Cargo resolves `kube-runtime` and the existing application compiles before the new reducer tests are added.

- [ ] **Step 2: Write failing reducer tests**

In the existing `#[cfg(test)] mod tests` in `src/k8s.rs`, import the watcher event type:

```rust
use kube::runtime::watcher;
```

Add these tests before defining `ServiceStore`:

```rust
#[test]
fn service_store_buffers_initial_snapshot_until_init_done() {
    let mut store = ServiceStore::default();
    let service = service(json!({WEBFINGER_KEY: "true"}), ports());

    assert!(store.apply(watcher::Event::Init).is_none());
    assert!(store
        .apply(watcher::Event::InitApply(service))
        .is_none());

    let snapshot = store
        .apply(watcher::Event::InitDone)
        .expect("complete initialization should publish");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].metadata.name.as_deref(), Some("test-service"));
}

#[test]
fn service_store_applies_and_deletes_live_events() {
    let mut store = ServiceStore::default();
    let initial = service(json!({WEBFINGER_KEY: "true"}), ports());
    store.apply(watcher::Event::Init);
    store.apply(watcher::Event::InitApply(initial));
    store.apply(watcher::Event::InitDone);

    let updated = service(
        json!({WEBFINGER_KEY: "true"}),
        json!([{ "name": "http", "port": 9090 }]),
    );
    let snapshot = store
        .apply(watcher::Event::Apply(updated))
        .expect("applied events should publish");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot[0].spec.as_ref().unwrap().ports.as_ref().unwrap()[0].port,
        9090
    );

    let deleted = service(json!({WEBFINGER_KEY: "true"}), ports());
    let snapshot = store
        .apply(watcher::Event::Delete(deleted))
        .expect("deleted events should publish");
    assert!(snapshot.is_empty());
}

#[test]
fn service_store_relist_replaces_stale_services_atomically() {
    let mut store = ServiceStore::default();
    let retained = service(json!({WEBFINGER_KEY: "true"}), ports());
    let stale = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "stale-service",
            "namespace": "apps",
            "annotations": {WEBFINGER_KEY: "true"}
        },
        "spec": {"ports": [{"name": "http", "port": 8081}]}
    }))
    .expect("test service should deserialize");

    store.apply(watcher::Event::Init);
    store.apply(watcher::Event::InitApply(retained.clone()));
    store.apply(watcher::Event::InitApply(stale));
    store.apply(watcher::Event::InitDone);

    store.apply(watcher::Event::Init);
    store.apply(watcher::Event::InitApply(retained));
    let snapshot = store
        .apply(watcher::Event::InitDone)
        .expect("relist completion should publish");
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.iter().all(|s| s.metadata.name.as_deref() != Some("stale-service")));
}
```

- [ ] **Step 3: Run the reducer tests and verify the red failure**

Run:

```bash
cargo test service_store_ -- --nocapture
```

Expected: compilation fails because `ServiceStore` is not yet defined. Do not proceed until the failure is caused by the missing reducer rather than an import or fixture error.

- [ ] **Step 4: Implement the minimal cache reducer**

Add this production reducer in `src/k8s.rs` before `start_reconciler`:

```rust
use std::collections::HashMap;

fn service_key(service: &Service) -> String {
    format!(
        "{}/{}",
        service.metadata.namespace.as_deref().unwrap_or("default"),
        service.metadata.name.as_deref().unwrap_or_default()
    )
}

#[derive(Default)]
struct ServiceStore {
    active: HashMap<String, Service>,
    pending: Option<HashMap<String, Service>>,
}

impl ServiceStore {
    fn apply(&mut self, event: watcher::Event<Service>) -> Option<Vec<Service>> {
        match event {
            watcher::Event::Init => {
                self.pending = Some(HashMap::new());
                None
            }
            watcher::Event::InitApply(service) => {
                self.pending
                    .get_or_insert_with(HashMap::new)
                    .insert(service_key(&service), service);
                None
            }
            watcher::Event::InitDone => {
                self.active = self.pending.take().unwrap_or_default();
                Some(self.snapshot())
            }
            watcher::Event::Apply(service) => {
                self.active.insert(service_key(&service), service);
                Some(self.snapshot())
            }
            watcher::Event::Delete(service) => {
                self.active.remove(&service_key(&service));
                Some(self.snapshot())
            }
        }
    }

    fn snapshot(&self) -> Vec<Service> {
        self.active.values().cloned().collect()
    }
}
```

Update the `kube` imports to include `Service`, `watcher`, and `WatchStreamExt`, and import `futures::StreamExt` for the reconciler task. Keep `backends_from_services` unchanged.

- [ ] **Step 5: Run the reducer tests and verify green**

Run:

```bash
cargo test service_store_ -- --nocapture
```

Expected: all three reducer tests pass. The `Init` sequence must return no snapshot before `InitDone`; a relist must remove the stale Service.

- [ ] **Step 6: Commit the self-contained cache change**

```bash
git add Cargo.toml Cargo.lock src/k8s.rs
git commit -m "feat: add Service watcher event store"
```

### Task 2: Integrate the resilient watcher and retry hint

**Files:**
- Modify: `src/k8s.rs:18,69-90` — replace the delayed ticker/list loop with watcher event processing and define the retry hint.
- Modify: `src/error.rs:53-58` — use the ten-second retry hint.
- Modify: `src/http.rs:215-223` — assert the exact retry hint in the existing no-backend test.

**Interfaces:**
- Consumes `ServiceStore::apply` from Task 1.
- Consumes `backends_from_services(Vec<Service>, &str) -> Vec<Backend>` unchanged.
- Produces `pub const RETRY_AFTER: Duration = Duration::from_secs(10)` for the `503` response.
- `start_reconciler(client, state, cluster_domain)` keeps its existing signature.

- [ ] **Step 1: Write the failing retry-header assertion**

Change the existing assertion in `test_no_backends_is_503_with_retry_after` to:

```rust
assert_eq!(
    resp.headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok()),
    Some("10")
);
```

- [ ] **Step 2: Run the focused test and verify red**

Run:

```bash
cargo test http::tests::test_no_backends_is_503_with_retry_after -- --nocapture
```

Expected: the test fails because the existing response still contains the old `30`-second value.

- [ ] **Step 3: Replace the obsolete sync interval constant**

In `src/k8s.rs`, replace:

```rust
pub const SYNC_INTERVAL: Duration = Duration::from_secs(30);
```

with:

```rust
pub const RETRY_AFTER: Duration = Duration::from_secs(10);
```

In `src/error.rs`, replace `crate::k8s::SYNC_INTERVAL` with `crate::k8s::RETRY_AFTER` and update the comment to say the `503` is transient while the watcher recovers.

- [ ] **Step 4: Make the retry-header test green**

Run:

```bash
cargo test http::tests::test_no_backends_is_503_with_retry_after -- --nocapture
```

Expected: PASS with the exact `Retry-After: 10` header.

- [ ] **Step 5: Replace polling with the kube runtime watcher**

Replace the body of `start_reconciler` with this event loop, preserving the existing function signature:

```rust
pub async fn start_reconciler(client: Client, state: Arc<BackendState>, cluster_domain: String) {
    let api: Api<Service> = Api::all(client);
    let stream = watcher(api, watcher::Config::default()).default_backoff();
    futures::pin_mut!(stream);
    let mut store = ServiceStore::default();

    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                if let Some(services) = store.apply(event) {
                    let backends = backends_from_services(services, &cluster_domain);
                    state.update(backends).await;
                }
            }
            Err(error) => {
                error!(error = %error, "Service watcher failed, retaining previous backends");
            }
        }
    }
}
```

Remove the `ListParams`, `TokioInstant`, `MissedTickBehavior`, and `interval_at` imports. Keep `Api`, `Client`, `Service`, `Duration`, and `Instant` imports needed by the remaining code. Do not add a second polling loop or clear `BackendState` in the error branch.

- [ ] **Step 6: Run the full application suite**

Run:

```bash
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Expected: all tests pass, clippy reports no warnings, and rustfmt reports no changes. If rustfmt changes import ordering in `src/k8s.rs`, apply those formatter changes and rerun all three commands.

- [ ] **Step 7: Commit the watcher integration**

```bash
git add src/k8s.rs src/error.rs src/http.rs
git commit -m "feat: watch Services for backend updates"
```

### Task 3: Whole-change verification and review

**Files:**
- Inspect: `Cargo.toml`, `Cargo.lock`, `src/k8s.rs`, `src/error.rs`, `src/http.rs`.
- No new files.

**Interfaces:**
- Verifies the public `start_reconciler` and `BackendState` APIs remain unchanged.
- Verifies the chart/CI release files are untouched by this runtime change.

- [ ] **Step 1: Verify the final diff scope and invariants**

Run:

```bash
git status --short --untracked-files=all
git diff --check HEAD~2..HEAD
git diff --name-only HEAD~2..HEAD
```

Expected modified runtime files are exactly `Cargo.toml`, `Cargo.lock`, `src/k8s.rs`, `src/error.rs`, and `src/http.rs`; no chart, CI, release, or unrelated source file appears.

Run:

```bash
if grep -nE 'SYNC_INTERVAL|interval_at|tokio::time::interval|ListParams' src/k8s.rs src/error.rs; then exit 1; fi
```

Expected: no matches. The reconciler must contain `watcher::Config::default()`, `.default_backoff()`, and all five event variants (`Init`, `InitApply`, `InitDone`, `Apply`, `Delete`).

- [ ] **Step 2: Verify runtime dependency and event behavior**

Run:

```bash
cargo tree -i kube-runtime
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Expected: `kube-runtime` is present, all tests pass, clippy and rustfmt are clean, and the combined diff has no whitespace errors.

- [ ] **Step 3: Review the two commits independently**

Inspect:

```bash
git log --oneline --decorate -4
git show --stat --oneline HEAD~1
git show --stat --oneline HEAD
```

Confirm that the cache reducer commit contains its tests and dependency feature, while the watcher integration commit contains only runtime wiring, retry-header behavior, and their tests.

- [ ] **Step 4: Record final acceptance**

The implementation is ready only when these statements are directly supported by command output:

- The first complete `Init` sequence publishes backends without a 30-second wait.
- `Apply` and `Delete` events publish changes without a periodic poll.
- A relist replaces stale Services only at `InitDone`.
- Watch errors leave the existing `BackendState` snapshot and readiness untouched.
- No periodic full-list ticker remains.
- All listed validation commands pass.
