-- migration:      0012
-- description:    rollback tenant registry, dek keystore, quota budgets, and audit log tables
-- author:         chief-strategist
-- date:           2026-11-06T09:48:00Z
-- depends_on:     0012
-- reversible:     YES
-- lock_risk:      LOW
-- rows_affected:  schema only
-- reason:         rollback WP-10 multi-tenant isolation tables

DROP TRIGGER IF EXISTS trg_prevent_tenant_isolation_audit_log_modification ON tenant_isolation_audit_log;
DROP FUNCTION IF EXISTS fn_prevent_tenant_isolation_audit_log_modification();
DROP TRIGGER IF EXISTS trg_prevent_tenant_quota_budgets_immutable_update ON tenant_quota_budgets;
DROP FUNCTION IF EXISTS fn_prevent_tenant_quota_budgets_immutable_update();
DROP TRIGGER IF EXISTS trg_prevent_tenant_dek_keystore_immutable_update ON tenant_dek_keystore;
DROP FUNCTION IF EXISTS fn_prevent_tenant_dek_keystore_immutable_update();
DROP TRIGGER IF EXISTS trg_prevent_tenant_registry_immutable_update ON tenant_registry;
DROP FUNCTION IF EXISTS fn_prevent_tenant_registry_immutable_update();

DROP TABLE IF EXISTS tenant_isolation_audit_log;
DROP TABLE IF EXISTS tenant_quota_budgets;
DROP TABLE IF EXISTS tenant_dek_keystore;
DROP TABLE IF EXISTS tenant_registry;

