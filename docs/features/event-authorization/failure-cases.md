# Event Authorization Failure Cases and Resilience Matrix

| Failure Mode | Root Cause | Detection | System Action | Recovery / Mitigation |
| :--- | :--- | :--- | :--- | :--- |
| **Missing Policy** | Principal attempts to publish or subscribe without provisioned policy. | Fail-closed policy evaluator. | Evaluation returns `DENIED`; emits security audit log. | Policy administrator provisions explicit policy via `POST /api/v1/auth/policies`. |
| **Revoked / Disabled Policy** | Policy status is set to `REVOKED` or `DISABLED`. | State check in `auth_policy_ledger`. | Immediate rejection of subsequent requests. | Audit log records denied access attempt with revoked policy reference. |
| **Resource Pattern Mismatch** | Principal requests action on `orders.payments.v1` but policy only permits `orders.cart.*`. | Exact and prefix regex policy matcher. | Returns `DENIED` with reason `RESOURCE_MISMATCH`. | Adjust resource pattern or issue dedicated policy. |
| **Expired Workload Identity** | JWT or SPIFFE certificate past validity window. | Identity verifier timestamp check. | Immediate rejection with `EXPIRED_CREDENTIAL`. | Workload requests new token via identity provider. |
| **Database Outage During Evaluation** | PostgreSQL connectivity interruption. | Timeout / connection pool exhaustion. | Fail-closed rejection: returns HTTP 503; prevents unauthorized bypass. | Auto-retry when database connectivity is restored. |
