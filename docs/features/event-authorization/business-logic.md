# Event Authorization — Business Logic Decision Tree

```
EVENT AUTHORIZATION (BUSINESS DECISION TREE)
│
├── 1. IDENTITY PROPAGATION & AUTHENTICATION TIER
│   └── Trigger: Inbound Request or Event Crosses Security Perimeter
│       ├── Layer A: External Client Identity (Edge Envoy Gateway)
│       │   ├── Authentication Mechanism: OIDC / OAuth2 Bearer JWT Token
│       │   ├── Envoy JWT Authentication Filter:
│       │   │   ├── Validates signature against JWKS endpoint
│       │   │   ├── Validates `iss` (Issuer), `aud` (Audience), and expiration (`exp > NOW()`)
│       │   │   ├── Valid ──> Extracts Claims (`sub`, `tenant_id`, `roles`, `permissions`)
│       │   │   └── Invalid / Expired ──> Reject with HTTP 401 Unauthorized
│       │   └── Ingestion Header Injection: Inject claims into internal headers (`X-User-ID`, `X-Roles`)
│       │
│       └── Layer B: Workload Identity (Inter-Service / Kafka Clients)
│           ├── Mechanism: mTLS X.509 Certificates / SPIFFE ID (`spiffe://cluster.local/ns/prod/sa/order-service`)
│           └── Kafka Broker Auth: SASL/OAUTHBEARER or SASL/SCRAM with TLS
│
├── 2. PRODUCER-SIDE AUTHORIZATION (PUBLISH ACCESS CONTROL)
│   └── Trigger: Service Emits Domain Event to Kafka Broker
│       ├── Kafka Broker ACL Evaluation:
│       │   ├── Rule: Least-Privilege Principal Binding
│       │   │   ```
│       │   │   kafka-acls.sh --add --allow-principal User:order-service \
│       │   │     --operation Write --topic orders.order.placed.v1 --resource-pattern-type literal
│       │   │   ```
│       │   ├── Check: Is publishing service principal authorized to write to target topic?
│       │   │   ├── YES ──> Broker accepts produce batch
│       │   │   └── NO ──> `TopicAuthorizationException` (Produce rejected)
│       │   │
│       │   └── Invariant: Service principals can write ONLY to their owned aggregate topics
│
└── 3. CONSUMER-SIDE AUTHORIZATION (SUBSCRIPTION ACCESS CONTROL)
    └── Trigger: Service Attempts to Subscribe to Kafka Topic
        ├── Kafka Broker ACL Subscription Evaluation:
        │   ├── Check 1: Topic Read Permission
        │   │   └── Principal must have `Operation: Read` on target topic
        │   ├── Check 2: Consumer Group Permission
        │   │   └── Principal must have `Operation: Read` on consumer group resource: `group/<group-name>`
        │   │
        │   ├── Authorization Check Fails:
        │   │   ├── Broker returns: `GroupAuthorizationException`
        │   │   └── Consumer loop terminates; security alarm emitted
        │   │
        │   └── Authorization Check Succeeds:
        │       └── Broker joins consumer to group; stream commences
        │
        └── Field-Level Data Governance Guard (Consumer Logic):
            ├── Even if topic is readable, consumer may not be authorized for PII fields
            ├── Check: Does consumer role contain `permission:view_pii`?
            │   ├── YES ──> Request KMS key to decrypt sensitive payload
            │   └── NO ──> PII fields suppressed / masked; only anonymized business metrics consumed
```
