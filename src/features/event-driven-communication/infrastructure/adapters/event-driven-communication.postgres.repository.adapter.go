package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"event-driven-platform/src/features/event-driven-communication/contracts"
	"event-driven-platform/src/features/event-driven-communication/infrastructure/ports"
	"event-driven-platform/src/features/event-driven-communication/infrastructure/queries"
)

var (
	ErrSagaDefinitionNotFound = errors.New("saga definition not found")
	ErrSagaInstanceNotFound   = errors.New("saga instance not found")
)

type PostgresEventCommunicationAdapter struct {
	db *sql.DB
}

func NewPostgresEventCommunicationAdapter(db *sql.DB) ports.EventCommunicationRepositoryPort {
	return &PostgresEventCommunicationAdapter{db: db}
}

func (a *PostgresEventCommunicationAdapter) SaveSagaDefinition(ctx context.Context, def contracts.SagaDefinition) error {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.SaveSagaDefinition")
	defer span.End()

	if a.db == nil {
		return nil
	}

	stepsJSON, err := json.Marshal(def.Steps)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	_, err = a.db.ExecContext(
		ctx,
		queries.InsertSagaDefinition,
		def.ID,
		def.PublicID,
		def.TenantID,
		def.SourceService,
		def.ExternalID,
		def.CorrelationID,
		def.SagaName,
		def.TotalSteps,
		string(stepsJSON),
		def.IsActive,
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

func (a *PostgresEventCommunicationAdapter) GetSagaDefinition(
	ctx context.Context,
	tenantID string,
	defID string,
) (contracts.SagaDefinition, error) {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.GetSagaDefinition")
	defer span.End()

	if a.db == nil {
		return contracts.SagaDefinition{
			PublicID:   defID,
			TenantID:   tenantID,
			TotalSteps: 1,
			IsActive:   true,
		}, nil
	}

	var def contracts.SagaDefinition
	var stepsJSON string

	err := a.db.QueryRowContext(ctx, queries.SelectSagaDefinition, tenantID, defID).Scan(
		&def.ID,
		&def.PublicID,
		&def.TenantID,
		&def.SourceService,
		&def.ExternalID,
		&def.CorrelationID,
		&def.SagaName,
		&def.TotalSteps,
		&stepsJSON,
		&def.IsActive,
		&def.CreatedAt,
		&def.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.SagaDefinition{}, ErrSagaDefinitionNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.SagaDefinition{}, err
	}

	_ = json.Unmarshal([]byte(stepsJSON), &def.Steps)
	return def, nil
}

func (a *PostgresEventCommunicationAdapter) CreateSagaInstance(
	ctx context.Context,
	inst contracts.SagaInstance,
	steps []contracts.SagaStepRecord,
) error {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.CreateSagaInstance")
	defer span.End()

	if a.db == nil {
		return nil
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	_, err = tx.ExecContext(
		ctx,
		queries.InsertSagaInstance,
		inst.ID,
		inst.PublicID,
		inst.TenantID,
		inst.SourceService,
		inst.ExternalID,
		inst.CorrelationID,
		inst.SagaDefinitionID,
		inst.CurrentStepIndex,
		string(inst.State),
		inst.DeadlineAt,
		now,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	for _, step := range steps {
		_, err = tx.ExecContext(
			ctx,
			queries.InsertSagaStep,
			step.ID,
			step.PublicID,
			step.TenantID,
			step.SourceService,
			step.ExternalID,
			step.CorrelationID,
			step.SagaInstanceID,
			step.StepIndex,
			step.StepName,
			string(step.StepType),
			string(step.State),
			step.CommandPayload,
			step.ReplyPayload,
			step.CompensationPayload,
			step.DeadlineAt,
			now,
			now,
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	return tx.Commit()
}

func (a *PostgresEventCommunicationAdapter) GetSagaInstance(
	ctx context.Context,
	tenantID string,
	instID string,
) (contracts.SagaInstance, error) {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.GetSagaInstance")
	defer span.End()

	if a.db == nil {
		return contracts.SagaInstance{
			PublicID:         instID,
			TenantID:         tenantID,
			CurrentStepIndex: 0,
			State:            contracts.SagaStateInProgress,
			DeadlineAt:       time.Now().Add(time.Hour),
		}, nil
	}

	var inst contracts.SagaInstance
	var stateStr string

	err := a.db.QueryRowContext(ctx, queries.SelectSagaInstance, tenantID, instID).Scan(
		&inst.ID,
		&inst.PublicID,
		&inst.TenantID,
		&inst.SourceService,
		&inst.ExternalID,
		&inst.CorrelationID,
		&inst.SagaDefinitionID,
		&inst.CurrentStepIndex,
		&stateStr,
		&inst.DeadlineAt,
		&inst.CreatedAt,
		&inst.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return contracts.SagaInstance{}, ErrSagaInstanceNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return contracts.SagaInstance{}, err
	}

	inst.State = contracts.SagaState(stateStr)
	return inst, nil
}

func (a *PostgresEventCommunicationAdapter) UpdateSagaInstanceState(
	ctx context.Context,
	params ports.UpdateSagaInstanceParams,
) error {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.UpdateSagaInstanceState")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.UpdateSagaInstanceState,
		string(params.State),
		params.NextStepIndex,
		now,
		params.TenantID,
		params.InstanceID,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (a *PostgresEventCommunicationAdapter) GetSagaSteps(
	ctx context.Context,
	tenantID string,
	instID string,
) ([]contracts.SagaStepRecord, error) {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.GetSagaSteps")
	defer span.End()

	if a.db == nil {
		return nil, nil
	}

	rows, err := a.db.QueryContext(ctx, queries.SelectSagaSteps, tenantID, instID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer rows.Close()

	var steps []contracts.SagaStepRecord
	for rows.Next() {
		var step contracts.SagaStepRecord
		var typeStr string
		var stateStr string

		if err := rows.Scan(
			&step.ID,
			&step.PublicID,
			&step.TenantID,
			&step.SourceService,
			&step.ExternalID,
			&step.CorrelationID,
			&step.SagaInstanceID,
			&step.StepIndex,
			&step.StepName,
			&typeStr,
			&stateStr,
			&step.CommandPayload,
			&step.ReplyPayload,
			&step.CompensationPayload,
			&step.DeadlineAt,
			&step.CreatedAt,
			&step.UpdatedAt,
		); err != nil {
			return nil, err
		}
		step.StepType = contracts.StepType(typeStr)
		step.State = contracts.StepState(stateStr)
		steps = append(steps, step)
	}
	return steps, nil
}

func (a *PostgresEventCommunicationAdapter) UpdateStepState(
	ctx context.Context,
	params ports.UpdateStepParams,
) error {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.UpdateStepState")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.UpdateSagaStepState,
		string(params.State),
		params.ReplyPayload,
		now,
		params.TenantID,
		params.InstanceID,
		params.StepIndex,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (a *PostgresEventCommunicationAdapter) SaveCommandReply(
	ctx context.Context,
	reply contracts.AsyncCommandReply,
) error {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.SaveCommandReply")
	defer span.End()

	if a.db == nil {
		return nil
	}

	now := time.Now().UTC()
	_, err := a.db.ExecContext(
		ctx,
		queries.InsertAsyncCommandReply,
		reply.ID,
		reply.PublicID,
		reply.TenantID,
		reply.SourceService,
		reply.ExternalID,
		reply.CorrelationID,
		reply.CommandID,
		string(reply.Outcome),
		reply.IsLateReply,
		reply.ReplyPayload,
		now,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}


func (a *PostgresEventCommunicationAdapter) SaveOutboundCommand(ctx context.Context, cmd ports.OutboundCommand) error {
	tr := otel.Tracer("event-driven-communication")
	ctx, span := tr.Start(ctx, "PostgresEventCommunicationAdapter.SaveOutboundCommand")
	defer span.End()

	if a.db == nil {
		return errors.New("database unavailable: cannot persist outbound command")
	}
	headers, err := json.Marshal(cmd.Headers)
	if err != nil {
		return err
	}
	_, err = a.db.ExecContext(
		ctx,
		queries.InsertOutboundCommandOutbox,
		cmd.ID,
		cmd.PublicID,
		cmd.TenantID,
		cmd.SourceService,
		cmd.ExternalID,
		cmd.CorrelationID,
		cmd.CausationID,
		cmd.AggregateType,
		cmd.AggregateID,
		cmd.EventType,
		cmd.SchemaVersion,
		cmd.Payload,
		string(headers),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return ports.ErrDuplicateCommand
		}
		return err
	}
	return nil
}
