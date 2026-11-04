package adapters

import (
	"context"
	"database/sql"
	"time"

	"event-driven-platform/src/features/event-versioning/contracts"
	"event-driven-platform/src/features/event-versioning/infrastructure/ports"
	"event-driven-platform/src/features/event-versioning/infrastructure/queries"
	"event-driven-platform/src/infra/database"
)

type PostgresEventVersioningAdapter struct {
	db *sql.DB
}

func NewPostgresEventVersioningAdapter(db *sql.DB) *PostgresEventVersioningAdapter {
	return &PostgresEventVersioningAdapter{db: db}
}

var _ ports.EventVersioningRepositoryPort = (*PostgresEventVersioningAdapter)(nil)

func (p *PostgresEventVersioningAdapter) SaveSchemaVersion(ctx context.Context, r contracts.SchemaVersionRecord) error {
	createdAt := r.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	_, err := p.db.ExecContext(ctx, queries.FlowInsertSchemaVersion,
		r.ID, r.PublicID, r.TenantID, r.SourceService, r.ExternalID, r.CorrelationID,
		r.StreamName, r.EventType, r.SchemaVersion, r.SchemaID, string(r.Format),
		string(r.CompatibilityMode), r.SchemaDefinition, r.PayloadHash, createdAt,
	)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresEventVersioningAdapter) GetLatestSchemaVersion(ctx context.Context, tenantID, streamName, eventType string) (*contracts.SchemaVersionRecord, error) {
	row := p.db.QueryRowContext(ctx, queries.FlowGetLatestSchemaVersion, tenantID, streamName, eventType)
	var rec contracts.SchemaVersionRecord
	var fmtStr, compatStr string
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&rec.StreamName, &rec.EventType, &rec.SchemaVersion, &rec.SchemaID, &fmtStr,
		&compatStr, &rec.SchemaDefinition, &rec.PayloadHash, &rec.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, database.MapDatabaseError(err)
	}
	rec.Format = contracts.SchemaFormat(fmtStr)
	rec.CompatibilityMode = contracts.CompatibilityMode(compatStr)
	return &rec, nil
}

func (p *PostgresEventVersioningAdapter) GetSchemaVersion(ctx context.Context, tenantID, streamName, eventType string, version int) (*contracts.SchemaVersionRecord, error) {
	row := p.db.QueryRowContext(ctx, queries.FlowGetSchemaVersion, tenantID, streamName, eventType, version)
	var rec contracts.SchemaVersionRecord
	var fmtStr, compatStr string
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&rec.StreamName, &rec.EventType, &rec.SchemaVersion, &rec.SchemaID, &fmtStr,
		&compatStr, &rec.SchemaDefinition, &rec.PayloadHash, &rec.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, database.MapDatabaseError(err)
	}
	rec.Format = contracts.SchemaFormat(fmtStr)
	rec.CompatibilityMode = contracts.CompatibilityMode(compatStr)
	return &rec, nil
}

func (p *PostgresEventVersioningAdapter) ListSchemaVersions(ctx context.Context, tenantID, streamName, eventType string) ([]contracts.SchemaVersionRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowListSchemaVersions, tenantID, streamName, eventType)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var list []contracts.SchemaVersionRecord
	for rows.Next() {
		var rec contracts.SchemaVersionRecord
		var fmtStr, compatStr string
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.StreamName, &rec.EventType, &rec.SchemaVersion, &rec.SchemaID, &fmtStr,
			&compatStr, &rec.SchemaDefinition, &rec.PayloadHash, &rec.CreatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		rec.Format = contracts.SchemaFormat(fmtStr)
		rec.CompatibilityMode = contracts.CompatibilityMode(compatStr)
		list = append(list, rec)
	}
	return list, nil
}

func (p *PostgresEventVersioningAdapter) SaveUpcaster(ctx context.Context, r contracts.UpcasterRecord) error {
	createdAt := r.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	_, err := p.db.ExecContext(ctx, queries.FlowInsertUpcaster,
		r.ID, r.PublicID, r.TenantID, r.SourceService, r.ExternalID, r.CorrelationID,
		r.EventType, r.FromVersion, r.ToVersion, r.TransformerName, r.IsActive, createdAt,
	)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (p *PostgresEventVersioningAdapter) GetUpcasterChain(ctx context.Context, tenantID, eventType string, fromVersion, toVersion int) ([]contracts.UpcasterRecord, error) {
	rows, err := p.db.QueryContext(ctx, queries.FlowGetUpcasterChain, tenantID, eventType, fromVersion, toVersion)
	if err != nil {
		return nil, database.MapDatabaseError(err)
	}
	defer rows.Close()

	var chain []contracts.UpcasterRecord
	for rows.Next() {
		var rec contracts.UpcasterRecord
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
			&rec.EventType, &rec.FromVersion, &rec.ToVersion, &rec.TransformerName, &rec.IsActive, &rec.CreatedAt,
		); err != nil {
			return nil, database.MapDatabaseError(err)
		}
		chain = append(chain, rec)
	}
	return chain, nil
}
