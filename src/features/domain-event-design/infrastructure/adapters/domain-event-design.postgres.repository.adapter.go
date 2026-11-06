package adapters

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"event-driven-platform/src/features/domain-event-design/contracts"
	"event-driven-platform/src/features/domain-event-design/domain/rules"
	"event-driven-platform/src/features/domain-event-design/infrastructure/ports"
	"event-driven-platform/src/features/domain-event-design/infrastructure/queries"
)

var (
	ErrCatalogNotFound   = errors.New("event catalog entry not found")
	ErrAggregateNotFound = errors.New("aggregate root version record not found")
	ErrCausalNotFound    = errors.New("causal chain entry not found")
)

type PostgresDomainEventAdapter struct {
	db *sql.DB
}

func NewPostgresDomainEventAdapter(db *sql.DB) ports.DomainEventRepositoryPort {
	return &PostgresDomainEventAdapter{db: db}
}

func (a *PostgresDomainEventAdapter) SaveCatalogEntry(ctx context.Context, entry contracts.EventCatalogEntry) error {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.SaveCatalogEntry")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.InsertCatalogEntry,
		entry.ID,
		entry.PublicID,
		entry.TenantID,
		entry.SourceService,
		entry.ExternalID,
		entry.CorrelationID,
		entry.EventType,
		entry.OwningAggregate,
		string(entry.SemanticsClass),
		string(entry.Visibility),
		string(entry.PIIClassification),
		entry.SchemaFingerprint,
		entry.IsActive,
		now,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (a *PostgresDomainEventAdapter) GetCatalogEntry(
	ctx context.Context,
	tenantID string,
	eventType string,
) (contracts.EventCatalogEntry, error) {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.GetCatalogEntry")
	defer span.End()

	if a.db == nil {
		return contracts.EventCatalogEntry{
			EventType:      eventType,
			TenantID:       tenantID,
			SemanticsClass: contracts.SemanticsClassState,
			Visibility:     contracts.EventVisibilityPrivate,
			IsActive:       true,
		}, nil
	}

	var entry contracts.EventCatalogEntry
	var semantics string
	var visibility string
	var pii string

	err := a.db.QueryRowContext(ctx, queries.SelectCatalogEntry, tenantID, eventType).Scan(
		&entry.ID,
		&entry.PublicID,
		&entry.TenantID,
		&entry.SourceService,
		&entry.ExternalID,
		&entry.CorrelationID,
		&entry.EventType,
		&entry.OwningAggregate,
		&semantics,
		&visibility,
		&pii,
		&entry.SchemaFingerprint,
		&entry.IsActive,
		&entry.CreatedAt,
		&entry.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.EventCatalogEntry{}, ErrCatalogNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.EventCatalogEntry{}, err
	}

	entry.SemanticsClass = contracts.SemanticsClass(semantics)
	entry.Visibility = contracts.EventVisibility(visibility)
	entry.PIIClassification = contracts.PIIClassification(pii)
	return entry, nil
}

func (a *PostgresDomainEventAdapter) GetAggregateVersion(
	ctx context.Context,
	tenantID string,
	aggType string,
	aggID string,
) (contracts.AggregateRootVersion, error) {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.GetAggregateVersion")
	defer span.End()

	if a.db == nil {
		return contracts.AggregateRootVersion{
			TenantID:       tenantID,
			AggregateType:  aggType,
			AggregateID:    aggID,
			CurrentVersion: 0,
			IsTerminal:     false,
		}, nil
	}

	var row contracts.AggregateRootVersion
	err := a.db.QueryRowContext(ctx, queries.SelectAggregateVersion, tenantID, aggType, aggID).Scan(
		&row.ID,
		&row.PublicID,
		&row.TenantID,
		&row.SourceService,
		&row.ExternalID,
		&row.CorrelationID,
		&row.AggregateType,
		&row.AggregateID,
		&row.CurrentVersion,
		&row.IsTerminal,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.AggregateRootVersion{
				TenantID:       tenantID,
				AggregateType:  aggType,
				AggregateID:    aggID,
				CurrentVersion: 0,
				IsTerminal:     false,
			}, nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.AggregateRootVersion{}, err
	}
	return row, nil
}

func (a *PostgresDomainEventAdapter) ReserveAggregateVersion(
	ctx context.Context,
	cmd contracts.ReserveAggregateVersionCommand,
) (contracts.ReserveAggregateVersionResult, error) {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.ReserveAggregateVersion")
	defer span.End()

	if a.db == nil {
		inc := cmd.VersionIncrement
		if inc <= 0 {
			inc = 1
		}
		return contracts.ReserveAggregateVersionResult{
			AggregateType:   cmd.AggregateType,
			AggregateID:     cmd.AggregateID,
			PreviousVersion: cmd.ExpectedVersion,
			ReservedVersion: cmd.ExpectedVersion + inc,
			IsTerminal:      cmd.MarkTerminal,
		}, nil
	}

	now := time.Now().UTC()
	initID := uuid.New()
	pubID := "agg_" + uuid.New().String()[:18]
	var corrID uuid.UUID
	if parsed, err := uuid.Parse(cmd.CorrelationID); err == nil {
		corrID = parsed
	} else {
		corrID = uuid.New()
	}

	_, _ = a.db.ExecContext(
		ctx,
		queries.UpsertInitialAggregateVersion,
		initID,
		pubID,
		cmd.TenantID,
		cmd.SourceService,
		cmd.ExternalID,
		corrID,
		cmd.AggregateType,
		cmd.AggregateID,
		0,
		false,
		now,
		now,
	)

	current, err := a.GetAggregateVersion(ctx, cmd.TenantID, cmd.AggregateType, cmd.AggregateID)
	if err != nil {
		return contracts.ReserveAggregateVersionResult{}, err
	}

	if current.IsTerminal {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrAggregateTerminal
	}

	if current.CurrentVersion != cmd.ExpectedVersion {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrOccConflict
	}

	inc := cmd.VersionIncrement
	if inc <= 0 {
		inc = 1
	}
	nextVersion := current.CurrentVersion + inc

	res, err := a.db.ExecContext(
		ctx,
		queries.UpdateAggregateVersionOCC,
		nextVersion,
		cmd.MarkTerminal,
		now,
		cmd.TenantID,
		cmd.AggregateType,
		cmd.AggregateID,
		cmd.ExpectedVersion,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.ReserveAggregateVersionResult{}, err
	}

	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return contracts.ReserveAggregateVersionResult{}, rules.ErrOccConflict
	}

	return contracts.ReserveAggregateVersionResult{
		AggregateType:   cmd.AggregateType,
		AggregateID:     cmd.AggregateID,
		PreviousVersion: current.CurrentVersion,
		ReservedVersion: nextVersion,
		IsTerminal:      cmd.MarkTerminal,
	}, nil
}

func (a *PostgresDomainEventAdapter) UpsertTransactionGroup(
	ctx context.Context,
	cmd contracts.RegisterTransactionGroupPartCommand,
) (contracts.RegisterTransactionGroupPartResult, error) {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.UpsertTransactionGroup")
	defer span.End()

	if a.db == nil {
		return contracts.RegisterTransactionGroupPartResult{
			TransactionGroupID: cmd.TransactionGroupID,
			ExpectedGroupSize:  cmd.ExpectedGroupSize,
			ReceivedCount:      1,
			IsComplete:         cmd.ExpectedGroupSize <= 1,
		}, nil
	}

	now := time.Now().UTC()
	initID := uuid.New()
	pubID := "txg_" + uuid.New().String()[:18]
	var corrID uuid.UUID
	if parsed, err := uuid.Parse(cmd.CorrelationID); err == nil {
		corrID = parsed
	} else {
		corrID = uuid.New()
	}

	var res contracts.RegisterTransactionGroupPartResult
	err := a.db.QueryRowContext(
		ctx,
		queries.UpsertTransactionGroup,
		initID,
		pubID,
		cmd.TenantID,
		cmd.SourceService,
		cmd.ExternalID,
		corrID,
		cmd.TransactionGroupID,
		cmd.ExpectedGroupSize,
		now,
	).Scan(
		&res.TransactionGroupID,
		&res.ExpectedGroupSize,
		&res.ReceivedCount,
		&res.IsComplete,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.RegisterTransactionGroupPartResult{}, err
	}
	return res, nil
}

func (a *PostgresDomainEventAdapter) RecordCausalEntry(ctx context.Context, entry contracts.CausalChainEntry) error {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.RecordCausalEntry")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.InsertCausalChainEntry,
		entry.ID,
		entry.PublicID,
		entry.TenantID,
		entry.SourceService,
		entry.ExternalID,
		entry.CorrelationID,
		entry.EventID,
		entry.CausationID,
		entry.CausationDepth,
		entry.CausationPathHash,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (a *PostgresDomainEventAdapter) GetCausalEntry(
	ctx context.Context,
	tenantID string,
	eventID string,
) (contracts.CausalChainEntry, error) {
	tr := otel.Tracer("domain-event-design")
	ctx, span := tr.Start(ctx, "PostgresDomainEventAdapter.GetCausalEntry")
	defer span.End()

	if a.db == nil {
		return contracts.CausalChainEntry{
			TenantID:       tenantID,
			EventID:        eventID,
			CausationDepth: 0,
		}, nil
	}

	var entry contracts.CausalChainEntry
	err := a.db.QueryRowContext(ctx, queries.SelectCausalChainEntry, tenantID, eventID).Scan(
		&entry.ID,
		&entry.PublicID,
		&entry.TenantID,
		&entry.SourceService,
		&entry.ExternalID,
		&entry.CorrelationID,
		&entry.EventID,
		&entry.CausationID,
		&entry.CausationDepth,
		&entry.CausationPathHash,
		&entry.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.CausalChainEntry{}, ErrCausalNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.CausalChainEntry{}, err
	}
	return entry, nil
}
