# Enterprise Event Catalog & Envelope Specification

## 1. Universal Event Envelope Specification (v1.0)

All events published across the platform MUST be wrapped in the standard envelope. Hand-crafted, bespoke message formats are strictly prohibited by CI linting and the envelope compiler library.

```json
{
  "event_id": "018f67e4-2b9a-7a11-b21a-d4c892b19280",
  "public_id": "evt_7f8a9b0c1d2e3f4a5b",
  "tenant_id": "tenant_enterprise_01",
  "source_service": "order-management-service",
  "external_id": "ord_ext_99218274",
  "correlation_id": "018f67e4-2b9a-7a11-b21a-d4c892b19281",
  "causation_id": "018f67e4-2b9a-7a11-b21a-d4c892b19282",
  "aggregate_type": "Order",
  "aggregate_id": "ord_8821948",
  "aggregate_version": 1,
  "event_type": "orders.order.placed.v1",
  "semantics_class": "DELTA",
  "visibility": "PUBLIC",
  "occurred_at": "2026-11-06T12:00:00.000000Z",
  "schema_version": "1.0.0",
  "schema_id": "sch_ord_placed_v1",
  "trace_context": {
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "tracestate": "rojo=1,congo=2"
  },
  "pii_classification": "RESTRICTED",
  "key_id": "key_tenant_enterprise_01_v2",
  "data": {
    "order_id": "ord_8821948",
    "customer_id": "cust_10928",
    "currency": "USD",
    "total_amount_cents": 12500,
    "items_count": 2,
    "shipping_address": {
      "country": "US",
      "postal_code": "94107"
    }
  }
}
```

### Field Role & Invariant Matrix

| Envelope Field | Type | Invariant Rule | Purpose |
|---|---|---|---|
| `event_id` | UUIDv7 | Globally unique, monotonically increasing | De-duplication key in consumer ledgers |
| `public_id` | String | Prefixed canonical identifier (`evt_*`) | Public reference in REST APIs and support audits |
| `tenant_id` | String | Multi-tenant boundary key | Tenant isolation, encryption key lookup |
| `source_service` | String | Originating microservice name | Provenance and authorization validation |
| `external_id` | String | Client-provided idempotency key | End-to-end client idempotency deduplication |
| `correlation_id` | UUIDv7 | Root transaction identifier | Distributed tracing and log stitching across services |
| `causation_id` | UUIDv7 | Immediate parent command/event ID | Causal tree verification and cycle detection |
| `aggregate_version` | Integer | Gapless integer ($n+1$) per aggregate | Sequence protection, reorder buffer ordering |
| `semantics_class` | Enum | `STATE`, `DELTA`, or `NOTIFICATION` | Binding consumer handler dispatch patterns |
| `trace_context` | Object | Valid W3C `traceparent` and `tracestate` | Distributed tracing propagation across hops |

---

## 2. Platform Event Catalog (Domain & Integration Events)

### Tier 0: Mission-Critical Financial & Transactional Streams

| Event Type Name | Owning Aggregate | Semantics Class | Visibility | Compatibility Mode | Partition Key Strategy | PII Class | Consumers |
|---|---|---|---|---|---|---|---|
| `orders.order.placed.v1` | `Order` | DELTA | PUBLIC | FULL_TRANSITIVE | `tenant_id:aggregate_id` | CONFIDENTIAL | PaymentService, InventoryService, NotificationService |
| `orders.order.cancelled.v1` | `Order` | DELTA | PUBLIC | FULL_TRANSITIVE | `tenant_id:aggregate_id` | INTERNAL | PaymentService, InventoryService, AccountingService |
| `payments.payment.authorized.v1` | `Payment` | DELTA | PUBLIC | BACKWARD_TRANSITIVE | `tenant_id:aggregate_id` | RESTRICTED (Card Info) | OrderService, FraudDetectionService |
| `payments.payment.captured.v1` | `Payment` | DELTA | PUBLIC | BACKWARD_TRANSITIVE | `tenant_id:aggregate_id` | RESTRICTED | OrderService, GeneralLedgerService |
| `payments.payment.declined.v1` | `Payment` | DELTA | PUBLIC | BACKWARD_TRANSITIVE | `tenant_id:aggregate_id` | RESTRICTED | OrderService, NotificationService |
| `inventory.stock.reserved.v1` | `InventoryItem` | DELTA | PRIVATE | BACKWARD_TRANSITIVE | `aggregate_id` | INTERNAL | OrderSagaOrchestrator |
| `inventory.stock.released.v1` | `InventoryItem` | DELTA | PRIVATE | BACKWARD_TRANSITIVE | `aggregate_id` | INTERNAL | OrderSagaOrchestrator |

### Tier 1: Core Operational & Async Command Streams

| Event Type Name | Owning Aggregate | Semantics Class | Visibility | Compatibility Mode | Partition Key Strategy | PII Class | Consumers |
|---|---|---|---|---|---|---|---|
| `customers.profile.updated.v1` | `Customer` | STATE | PUBLIC | FULL_TRANSITIVE | `tenant_id:aggregate_id` | RESTRICTED (PII) | SearchProjectionService, MarketingService |
| `customers.profile.erased.v1` | `Customer` | NOTIFICATION | PUBLIC | FULL_TRANSITIVE | `tenant_id:aggregate_id` | RESTRICTED | EncryptionKeyManager (Crypto-shred) |
| `shipping.fulfillment.dispatched.v1` | `Shipment` | DELTA | PUBLIC | BACKWARD_TRANSITIVE | `tenant_id:aggregate_id` | CONFIDENTIAL (Address) | TrackingService, NotificationService |
| `shipping.fulfillment.delivered.v1` | `Shipment` | STATE | PUBLIC | BACKWARD_TRANSITIVE | `tenant_id:aggregate_id` | INTERNAL | OrderService, AnalyticsService |
| `ledger.entry.posted.v1` | `JournalEntry` | DELTA | PRIVATE | BACKWARD_TRANSITIVE | `tenant_id:aggregate_id` | CONFIDENTIAL | BalanceProjectionService, TaxReporting |

---

## 3. Semantics Classification Matrix

1. **STATE (Event-Carried State Transfer):**
   - Carries the complete post-mutation snapshot of the aggregate.
   - Idempotent by design: consumers can overwrite their projection state directly without querying upstream services.
   - Example: `customers.profile.updated.v1`.

2. **DELTA (Domain Facts / State Transitions):**
   - Carries only the incremental delta and business parameters of what occurred.
   - Strict ordering required; consumers must apply deltas sequentially.
   - Example: `payments.payment.captured.v1`.

3. **NOTIFICATION (Lightweight Signal):**
   - Carries only identity keys and timestamps.
   - Informs consumers that an event occurred; consumers fetch details if authorized.
   - Example: `customers.profile.erased.v1`.

