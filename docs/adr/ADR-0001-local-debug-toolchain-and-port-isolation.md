# ADR-0001: Isolate Local Debugger Runtime Ports and Modernize Go Toolchain

* **Status**: Accepted
* **Date**: 2026-11-07
* **Deciders**: Platform Architecture Core Team
* **Technical Domain**: Developer Experience, Runtime Infrastructure, Toolchain Compatibility

---

## Context and Problem Statement

How can engineers run native Delve debugger sessions for `event-driven-platform` without colliding with the active Docker Compose infrastructure (specifically Envoy proxy listening on `:8080`) or suffering DWARF symbol table desynchronization between Go compiler toolchains and Delve?

During initial developer debugging, Delve aborted execution because:
1. `public listener failed: listen tcp :8080: bind: address already in use` caused by `event_platform-envoy-1` holding host socket `0.0.0.0:8080`.
2. Delve 1.27.2 enforced a minimum supported compiler baseline of Go 1.25, while the project's `go.mod` was pinned to Go 1.23.1.

---

## Decision Drivers

* **Zero Infrastructure Disruption**: Must not require shutting down background Docker Compose containers (Envoy, Kafka, PostgreSQL, OpenSearch, MinIO).
* **Deterministic Configuration Parity**: Production containers must continue to listen on default ports (`:8080`), while local debuggers bind dynamically.
* **Symbol Table Integrity**: Debugger must not operate under version-suppression flags (`--check-go-version=false`) during concurrent goroutine inspection.
* **Single-Step VS Code Ergonomics**: Developers must be able to hit `F5` in VS Code and debug HTTP handlers or unit tests without manual shell exports.

---

## Considered Options

### Option 1: Terminate the Envoy Container (`docker stop event_platform-envoy-1`)
* *Eliminated*: Halting Envoy frees `:8080`, but disables external gateway routing, breaks end-to-end integration tests, and forces manual container restarts when toggling between debugging and system integration.

### Option 2: Hardcode Static Non-Standard Port in `config/default.yaml`
* *Eliminated*: Changing YAML defaults directly to `8088` causes containerized deployments to bind away from Envoy's upstream cluster target (`app:8080`), breaking the container networking contract.

### Option 3: Rely Exclusively on `--check-go-version=false`
* *Eliminated*: Suppressing Delve's version gate leaves DWARF location lists and Go runtime internal struct definitions (`runtime.g`, `runtime.hchan`) mismatched, causing corrupted stack traces and phantom variable watches.

### Option 4: Parameterize Chassis Ports via Environment Variables, Modernize Toolchain to Go 1.26.10, and Anchor VS Code Launch Configurations (Selected)
* *Selected*: Decouples runtime binding from hardcoded strings, leverages Go's automatic toolchain management, and isolates developer tooling cleanly.

---

## Decision

We will:
1. Update `event-driven-platform/config/default.yaml` to dynamically resolve `bind_address: "${SERVER_BIND_ADDRESS:-0.0.0.0:8080}"` and `admin_bind_address: "${SERVER_ADMIN_BIND_ADDRESS:-127.0.0.1:8081}"`.
2. Configure `.vscode/launch.json` to inject `PORT: "8088"` and `ADMIN_PORT: "8089"`, matching `src/infra/chassis/runner.go`'s 12-factor environment parsing.
3. Update `event-driven-platform/go.mod` to declare `go 1.26.0` with `toolchain go1.26.10` to guarantee 100% compiler-debugger ABI alignment with Delve 1.27.2.

---

## Failure-First Analysis & Failure Modes

| Failure Archetype | Trigger Condition | Detection Signal | Prevention / Mitigation Mechanism |
| :--- | :--- | :--- | :--- |
| **Port Collision** | Running debugger while Docker Envoy or another dev server occupies port `8080`. | Process exits immediately with status code `70` and `listen tcp :8080: bind: address already in use`. | Debugger profile injects `PORT=8088` and `ADMIN_PORT=8089`. Docker remains on `8080`. |
| **Toolchain Desync** | Compiler generates DWARF symbols that Delve cannot decode. | Local variables display `<optimized out>`, pointers read as `nil`, or breakpoints fail to latch. | Enforce `toolchain go1.26.10` in `go.mod`, aligning compiler with Delve 1.27.2 baseline. |
| **Silent Traffic Routing Miss** | Developer fires requests to `http://localhost:8080` expecting breakpoints to hit on `8088`. | HTTP request returns Envoy 503 or routes to Docker, but debugger never pauses. | Documentation and ADR mandate pointing debug clients directly to `http://localhost:8088/api/v1`. |

---

## Consequences

### Positive
* Native single-click debugging in VS Code for both HTTP servers and individual hexagonal unit tests.
* Docker infrastructure runs uninterrupted in the background.
* High-fidelity goroutine and stack unwinding verified with clean compilation under Go 1.26.10.

### Negative
* API testing during active Delve sessions must explicitly target port `8088` rather than the gateway port `8080`.

### Neutral
* Local `.vscode/launch.json` is preserved as standard developer workspace tooling.

