# Event Authorization — Business Logic Decision Tree

```
EVENT AUTHORIZATION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (verify per version/vendor)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-IDP   Identity provider (OIDC) — user authentication, token issuance, JWKS
│   ├── C-GW    Edge gateway (Envoy) — token validation, header sanitation, token exchange call, rate limits
│   ├── C-STS   Internal token service — token exchange to audience-restricted internal tokens with delegation chain
│   ├── C-PDP   Policy decision point (policy as code) — resource-level decisions, decision logs
│   ├── C-PEP   Enforcement points — gateway, service handlers, query layer, decrypt layer, control plane
│   ├── C-WID   Workload identity (SPIFFE/SPIRE) — service identities for mTLS and Kafka where supported
│   ├── C-KAUTH Kafka authorizer + ACL-as-code — principals, resource patterns, drift detection
│   ├── C-DEC   Decryption policy layer / KMS key policies — field-class entitlements (encryption tree)
│   ├── C-CTL   Control-plane authorization — offsets, DLQ, replay, schema registry, connectors, keys
│   └── C-AUD   Audit trail — authorization decisions, denials, privileged actions (audit tree)
│
├── 1. INVARIANTS  (with enforcement)
│   ├── AZ-01 Identity is never trusted from plain headers [C]
│   │   └── Enforced by: gateway strips all identity headers from inbound requests; services accept identity only
│   │                   from verified signed tokens (C-STS) bound to mTLS peer identity
│   ├── AZ-02 Tokens are audience-restricted per hop [C]
│   │   └── Enforced by: token exchange per downstream audience; services reject tokens not addressed to them
│   ├── AZ-03 Default deny everywhere [C]
│   │   └── Enforced by: mesh policies, Kafka allow-only ACLs (no allow-everyone), PDP default deny
│   ├── AZ-04 Single writer per aggregate stream [C]
│   │   └── Enforced by: exactly one producer principal with Write on each K-ORDERED topic (ACL-as-code check)
│   ├── AZ-05 Authorization for business actions happens at command time on authoritative state [C]
│   │   └── Enforced by: command handlers call PDP with resource attributes from the write model (CQRS tree CQ-02)
│   ├── AZ-06 Events are authorized facts; consumers never trust event fields for privileged targets blindly [C]
│   │   └── Enforced by: privileged downstream effects use authoritative data, not free event fields (§7.4)
│   ├── AZ-07 Field-level access enforced by keys, not consumer code [C]
│   │   └── Enforced by: C-DEC entitlements / KMS key policies per field class
│   ├── AZ-08 Revocation has an SLO [C]
│   │   └── Enforced by: short token TTLs, decision cache TTLs ≤ SLO, revocation push, introspection for high-risk ops
│   ├── AZ-09 Control-plane actions are separately authorized, approved, and audited [C]
│   │   └── Enforced by: C-CTL with role separation and four-eyes for T0 operations
│   ├── AZ-10 Authorization configuration is code with drift detection [C]
│   │   └── Enforced by: ACLs, mesh policies, PDP policies, KMS policies in repos; diff vs live; alerts
│   └── AZ-11 Every deny and every privileged allow is auditable [C]
│       └── Enforced by: decision logs → audit trail (T-DENIED, T-ACCESS classes)
│
├── 2. EDGE AUTHENTICATION  (C-GW)
│   └── Trigger: External request arrives
│       ├── Check 2.1: Strip inbound identity headers (X-User-ID, X-Roles, X-Tenant-ID, forwarded identity) — always
│       ├── Check 2.2: Token present and well-formed?
│       │   ├── NO → 401
│       │   └── YES → Check 2.3
│       ├── Check 2.3: Token validation
│       │   ├── Algorithm in allow-list (asymmetric only; reject "none" and unexpected algorithms) [C]
│       │   ├── Key id resolves in cached JWKS
│       │   │   ├── Unknown key id → refresh JWKS once (rate-limited) → still unknown → 401
│       │   │   └── JWKS endpoint unavailable → use cached keys until max staleness → then fail closed
│       │   ├── Signature valid
│       │   ├── Issuer exact match; audience includes this API; token type = access token (not ID token) [C]
│       │   ├── exp, nbf, iat with bounded clock skew [L: ≤ 60 s]
│       │   └── Any failure → 401 with no detail leakage
│       ├── Check 2.4: Revocation / high-risk operations
│       │   ├── Low risk → rely on short TTL [L: 5–15 min access tokens]
│       │   └── High risk (payments, role changes, exports) → introspection or session check at C-IDP
│       ├── Check 2.5: Tenant context
│       │   ├── Single-tenant user → tenant from token
│       │   ├── Multi-tenant user → requested tenant validated against membership claim/service (never header trust)
│       │   └── Mismatch → 403 + security event
│       ├── Check 2.6: Token exchange → internal token (C-STS)
│       │   └── Contains: subject, tenant, roles/scopes snapshot, delegation chain, audience = target service, short TTL
│       │
│       └── Edge cases
│           ├── E2.1 Gateway forwards client-supplied X-User-ID — identity forgery — SECURITY [C]
│           ├── E2.2 Algorithm confusion (HMAC verified with public key as secret) — forged tokens accepted — SECURITY [C]
│           ├── E2.3 ID token accepted as access token — tokens issued to other clients usable here — SECURITY [C]
│           ├── E2.4 JWKS rotation: new key published, gateway cache stale — valid tokens rejected — OUTAGE [C]
│           │   └── Handling: refresh-on-unknown-kid with rate limit; IdP publishes new keys before use (overlap)
│           ├── E2.5 IdP outage — new logins fail; existing tokens valid until exp — DEGRADE (by design) [C]
│           ├── E2.6 Clock skew on gateway — valid tokens rejected or expired tokens accepted [C]
│           ├── E2.7 Role removed but token valid for its TTL — unauthorized actions — SECURITY [C]
│           │   └── Handling: AZ-08; high-risk actions use fresh decisions (Check 2.4)
│           └── E2.8 Oversized tokens (many roles) exceed header limits — sporadic 431/400 — OUTAGE for some users [C]
│               └── Handling: permissions resolved by PDP, not embedded wholesale in tokens
│
├── 3. IDENTITY PROPAGATION INSIDE THE PLATFORM
│   ├── 3.1 Two identities on every internal call
│   │   ├── Workload identity (mTLS peer SPIFFE ID) — "which service is calling"
│   │   └── Principal context (internal token) — "on whose behalf, which tenant"
│   ├── 3.2 Service-only calls (jobs, internal maintenance) → workload identity only; explicit "system" principal
│   ├── 3.3 Delegation chain: user → svc A → svc B recorded in token; each hop exchanges for next audience
│   ├── 3.4 Async boundary (into events): principal context captured as event metadata (actor, tenant, chain)
│   │   for audit and downstream decisions — it is a RECORD of authorization, not a credential
│   │
│   └── Edge cases
│       ├── E3.1 User token forwarded unchanged to all services — any service replays it elsewhere — SECURITY [C]
│       ├── E3.2 Internal token not bound to mTLS peer — stolen token usable from any workload — SECURITY [L]
│       │   └── Handling: token binding to workload identity (audience + caller identity checks)
│       ├── E3.3 Delegation chain dropped at a hop — downstream sees only service identity — over-privileged actions [C]
│       └── E3.4 Event metadata "actor" used later as a credential (consumer acts with user's rights) — SECURITY [C]
│
├── 4. SERVICE-TO-SERVICE AUTHORIZATION
│   ├── 4.1 Mesh policy: allowed (caller SPIFFE ID → callee service, method) pairs; default deny (AZ-03)
│   ├── 4.2 Service handler: verify internal token audience = self; verify caller identity matches allowed callers for method
│   ├── 4.3 Admin/debug endpoints: separate listener, separate policy, never exposed to mesh-wide callers
│   │
│   └── Edge cases
│       ├── E4.1 Namespace-wide allow rules — any compromised pod calls payment APIs — SECURITY [C]
│       ├── E4.2 Health/metrics/debug endpoints exposing data or actions — SECURITY [C]
│       └── E4.3 SPIFFE ID patterns too broad in policy (prefix match on namespace) — over-allow [C]
│
├── 5. COMMAND AUTHORIZATION  (AZ-05)
│   ├── Check 5.1: Action permission (role/scope) for principal in tenant
│   ├── Check 5.2: Resource-level permission using authoritative attributes (owner, tenant, state, amount limits)
│   │   └── Attributes read from write model in the same transaction as the decision (no read-model inputs)
│   ├── Check 5.3: Contextual risk (step-up auth for high-value actions, MFA freshness)
│   ├── Check 5.4: PDP availability
│   │   ├── Decision cache hit within TTL → use
│   │   └── PDP unavailable → fail closed for mutations; queries per endpoint policy
│   ├── Result: ALLOW → proceed; DENY → audit T-DENIED, 403
│   │
│   └── Edge cases
│       ├── E5.1 Authorization uses read-model attributes (stale owner/tenant) — wrong decisions — SECURITY [C]
│       ├── E5.2 IDOR: resource id from request not checked against principal's tenant/ownership — SECURITY [C]
│       ├── E5.3 Decision cache keyed without tenant — cross-tenant permission reuse — SECURITY [C]
│       └── E5.4 Bulk endpoints authorize the batch, not each item — partial unauthorized writes — SECURITY [C]
│
├── 6. KAFKA AUTHORIZATION
│   ├── 6.1 Principals
│   │   ├── mTLS principals derived from certificate subject via explicit mapping rules [C]
│   │   ├── SASL principals (OAUTHBEARER/SCRAM) with consistent naming
│   │   └── One principal per service per environment; humans never use service principals
│   │
│   ├── 6.2 Resources & operations by client type
│   │   ├── Producer (non-transactional): Write (+ Describe) on its topics [C]
│   │   ├── Producer (transactional): above + Write/Describe on its transactional id (prefixed by service) [C]
│   │   ├── Consumer: Read (implies Describe) on topics + Read on its consumer group(s) (prefixed by service) [C]
│   │   ├── Connect/CDC: Write on target topics, Read/Write on internal topics, group access for its cluster
│   │   ├── Admin tooling (C-CTL): group offset operations, topic config — separate principal, approvals
│   │   └── Nobody: Create on cluster in production (topics via IaC only); no DeleteRecords except governed tooling
│   │
│   ├── 6.3 Broker configuration
│   │   ├── allow.everyone.if.no.acl.found = false (default) — verified [C]
│   │   ├── super.users minimal (break-glass identities only), reviewed [C]
│   │   ├── No wildcard principal ACLs; no wildcard topic Write
│   │   └── Topic auto-creation disabled
│   │
│   ├── 6.4 ACL-as-code (AZ-10)
│   │   ├── Declared per service: owned topics (Write), subscribed topics (Read), groups/transactional ids (prefixed)
│   │   ├── CI checks: single writer per K-ORDERED topic (AZ-04); no T0 topic readable by T2 services unless approved
│   │   └── Drift detection: live ACLs vs repo; unexpected ACL → page
│   │
│   └── Edge cases
│       ├── E6.1 Two principals with Write on an aggregate topic — forged/duplicate events — CORRUPT/SECURITY [C]
│       ├── E6.2 Group Read granted broadly — another service joins group, steals partitions — LOSS/SECURITY [C]
│       ├── E6.3 Missing transactional-id ACL found in prod — emergency broad grant — SECURITY [C]
│       ├── E6.4 ACL removed during deploy — consumer fails to start — OUTAGE (not attack) [C]
│       │   └── Handling: classify authz failures (config drift vs unexpected principal); deploy pre-checks
│       ├── E6.5 Retry/DLQ/replay topics lack ACLs mirroring main topics — data exposure or broken pipelines [C]
│       ├── E6.6 Mirrored DR cluster with different ACLs — post-failover exposure or outage [C]
│       └── E6.7 Principal mapping rule change — every principal name changes — mass authorization failure — OUTAGE [C]
│
├── 7. EVENT PROVENANCE & CONSUMER TRUST
│   ├── 7.1 Facts
│   │   ├── Records carry no producer identity [C]
│   │   └── Provenance = write ACLs (AZ-04) + event contract (aggregate type owned by one service)
│   │
│   ├── 7.2 T0 events with direct financial effect (payment captured, payout approved)
│   │   ├── Option A: strict single-writer ACLs + continuous drift detection (baseline)
│   │   ├── Option B: producer-signed events (signature over canonical envelope + payload, key per producer) [L]
│   │   │   └── Consumers verify signature and producer key binding to aggregate type
│   │   └── Decision per stream [G: based on fraud risk]
│   │
│   ├── 7.3 Consumer validation of event semantics
│   │   ├── Event type allowed on this stream; aggregate type matches topic; tenant consistent (multi-tenant MT-02)
│   │   └── Business plausibility for T0 (amount limits, state transitions valid)
│   │
│   ├── 7.4 Confused deputy prevention (AZ-06)
│   │   ├── Privileged downstream actions (send money, send email to address, grant access) derive targets from
│   │   │   authoritative records, not from free-form event fields supplied by end users
│   │   └── Example: "report requested" event → recipient = registered email of requester (lookup), not event's email field
│   │
│   └── Edge cases
│       ├── E7.1 Admin uses console tool to produce a "PaymentCaptured" for testing in prod — real fulfillment — CORRUPT [C]
│       │   └── Handling: humans have no produce rights on business topics; break-glass via C-CTL with approvals
│       ├── E7.2 Compromised producer service emits valid-looking events — signatures don't help (key compromised) [C]
│       │   └── Handling: plausibility checks, anomaly detection, key revocation, reconciliation with providers
│       └── E7.3 Event field used as redirect/target for privileged effect — data exfiltration — SECURITY [C]
│
├── 8. AUTHORIZATION OVER TIME
│   ├── 8.1 Command-time decision binds the fact (AZ-05); consumers don't re-authorize facts
│   ├── 8.2 Downstream actions that deliver data to a person (exports, notifications with PII)
│   │   └── Re-check entitlement at delivery time (revocation may have happened during backlog)
│   ├── 8.3 Long backlogs / replays
│   │   ├── Replay never re-executes user-delivering effects (replay tree RP-02)
│   │   └── Redrive of DLQ items with delayed effects → entitlement re-check for user-facing deliveries
│   ├── 8.4 Revocation SLO (AZ-08) across: tokens, decision caches, read-side permissions (CQRS tree CQ-10), KMS grants
│   │
│   └── Edge cases
│       ├── E8.1 Export queued before access revoked, delivered after — data to unauthorized person — SECURITY [C]
│       └── E8.2 Projection embeds permissions — revoked user sees data until catch-up — SECURITY [C]
│
├── 9. FIELD-LEVEL & DATA AUTHORIZATION  (AZ-07)
│   ├── 9.1 Field classes (contact PII, financial, identity documents, health) with separate keys/entitlements
│   ├── 9.2 Enforcement at decrypt (C-DEC / KMS key policy); unentitled consumers cannot decrypt
│   ├── 9.3 Read APIs: field masking by entitlement at query layer (authoritative check at read time)
│   ├── 9.4 Derived data: views storing decrypted fields inherit the strictest entitlement of their sources
│   │
│   └── Edge cases
│       ├── E9.1 Consumer-side "if role then mask" after decryption — plaintext already exposed — SECURITY [C]
│       ├── E9.2 "Anonymized metrics" computed from pseudonymous ids — re-identifiable — COMPLIANCE [C]
│       └── E9.3 Search index of decrypted PII readable by broad internal roles — SECURITY [C]
│
├── 10. CONTROL-PLANE AUTHORIZATION  (AZ-09)
│   ├── Operations: offset changes, DLQ redrive/discard, replay runs, schema registration/rules, connector config,
│   │   ACL changes, key destruction, tenant offboarding, retention/object-lock changes
│   ├── Roles separated: operator, approver, security admin; no single person can approve own T0 action
│   ├── Break-glass: personal, time-bound, reason-coded, audited, auto-revoked
│   └── Edge cases
│       ├── E10.1 Direct Kafka admin access bypasses C-CTL — unaudited offset/ACL changes — SECURITY [C]
│       └── E10.2 Break-glass account left active — standing super-privilege — SECURITY [C]
│
├── 11. CREDENTIAL & IDENTITY LIFECYCLE
│   ├── Service credentials rotated automatically (certs via SPIRE; SASL secrets with overlap windows)
│   ├── Service decommission removes: ACLs, mesh policies, KMS grants, PDP roles, credentials
│   ├── Human offboarding removes all roles and break-glass eligibility within SLO
│   └── Edge cases
│       ├── E11.1 Orphaned service principal with Write ACLs after service removal — latent forgery path — SECURITY [C]
│       └── E11.2 Credential rotation without overlap — mass auth failures — OUTAGE [C]
│
├── 12. POLICY MANAGEMENT
│   ├── Policies as code with unit tests (allow and deny cases), reviewed changes, versioned rollout
│   ├── Decision logs sampled for allows, full for denies and privileged allows
│   ├── PDP availability: replicated, local caching sidecars; fail-closed for mutations
│   └── E12.1 Policy change widens access unintentionally — SECURITY [C]
│       └── Handling: policy diff tests showing new allowed (principal, action, resource) sets
│
├── 13. MULTI-TENANT SPECIFICS
│   ├── Tenant derived from token/membership, never from request input (multi-tenant MT-01)
│   ├── Cross-tenant admin (platform support) via impersonation with audit (audit tree §3.1)
│   └── E13.1 Tenant switch in UI sends tenant id header trusted by services — cross-tenant access — SECURITY [C]
│
├── 14. OBSERVABILITY & DETECTION
│   ├── Edge: 401/403 rates, invalid token reasons (alg, aud, exp, signature), JWKS refreshes
│   ├── Internal: mesh denies by caller/callee, token audience mismatches, missing tokens
│   ├── Kafka: authorization failures by principal/resource, ACL drift, new principals observed
│   ├── PDP: decision latency, deny rates, cache hit rate, unavailability
│   ├── Decrypt: entitlement denials, decrypt volume anomalies
│   ├── Control plane: privileged operations, break-glass activations
│   └── Absence alerts on all of the above
│
├── 15. RUNBOOKS
│   ├── RB-01 Spoofed identity header detected → block path → audit affected requests → fix stripping
│   ├── RB-02 Unexpected Kafka writer principal → revoke ACL → quarantine affected events → reconcile
│   ├── RB-03 JWKS rotation failures → refresh caches → coordinate with IdP overlap → monitor 401s
│   ├── RB-04 Mass authorization failures after deploy → identify ACL/policy/mapping change → roll back
│   ├── RB-05 Revocation incident (access after removal) → invalidate caches/tokens → audit actions in window
│   └── RB-06 Compromised service credential → revoke identity → rotate keys → review its writes and reads
│
└── 16. TEST MATRIX
    ├── Client sends X-User-ID header → stripped; downstream sees verified identity only
    ├── Direct pod call without internal token → rejected by service
    ├── Token with alg "none" / HMAC-with-public-key → rejected
    ├── ID token presented as access token → rejected
    ├── JWKS key rotation → no 401 spike
    ├── Role removed → high-risk action denied immediately; low-risk within token TTL
    ├── Multi-tenant user switches tenant without membership → 403
    ├── Second principal granted Write on aggregate topic → CI blocks; live drift alert
    ├── Foreign service joins another's consumer group → denied (prefixed group ACLs)
    ├── Human produces to business topic → denied
    ├── Unentitled consumer requests PII decrypt → denied at key layer
    ├── Export queued before revocation → delivery re-check denies
    ├── Report event with attacker-supplied email → delivery uses registered address
    ├── Principal mapping rule change in staging → detected as mass rename before prod
    └── Decommissioned service → all ACLs, policies, grants, credentials removed
```
