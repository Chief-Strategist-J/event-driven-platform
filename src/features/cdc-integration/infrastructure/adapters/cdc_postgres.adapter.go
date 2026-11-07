package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"event-driven-platform/src/features/cdc-integration/contracts"
	"event-driven-platform/src/features/cdc-integration/infrastructure/queries"
	"event-driven-platform/src/infra/database"
	"event-driven-platform/src/shared/errors"
	"event-driven-platform/src/shared/utils"
)

type PostgresCDCRepositoryAdapter struct {
	db *sql.DB
}

func NewPostgresCDCRepositoryAdapter(db *sql.DB) *PostgresCDCRepositoryAdapter {
	return &PostgresCDCRepositoryAdapter{db: db}
}

func (a *PostgresCDCRepositoryAdapter) RegisterConnector(ctx context.Context, record contracts.ConnectorInventoryRecord) error {
	_, err := a.db.ExecContext(ctx, queries.InsertConnectorInventory,
		record.ID, record.PublicID, record.TenantID, record.SourceService, record.ExternalID, record.CorrelationID,
		record.ConnectorName, record.SlotName, record.PublicationName, string(record.Mode), string(record.State),
	)
	if err != nil {
		return database.MapDatabaseError(err)
	}
	return nil
}

func (a *PostgresCDCRepositoryAdapter) GetConnector(ctx context.Context, connectorName string) (*contracts.ConnectorInventoryRecord, error) {
	row := a.db.QueryRowContext(ctx, queries.SelectConnectorByName, connectorName)

	var rec contracts.ConnectorInventoryRecord
	var modeStr, stateStr string
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SourceService, &rec.ExternalID, &rec.CorrelationID,
		&rec.ConnectorName, &rec.SlotName, &rec.PublicationName, &modeStr, &stateStr,
		&rec.WALRetainedBytes, &rec.WALLagBytes, &rec.LastHeartbeatAt, &rec.Version,
		&rec.CreatedAt, &rec.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.NewNotFoundError(fmt.Sprintf("cdc connector not found: %s", connectorName))
		}
		return nil, errors.NewInternalError(fmt.Sprintf("failed to query cdc connector inventory: %v", err))
	}

	rec.Mode = contracts.CDCMode(modeStr)
	rec.State = contracts.CDCConnectorState(stateStr)
	return &rec, nil
}

func (a *PostgresCDCRepositoryAdapter) UpdateConnectorState(ctx context.Context, connectorName string, state contracts.CDCConnectorState, version int64) error {
	res, err := a.db.ExecContext(ctx, queries.UpdateConnectorState, string(state), connectorName, version)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to update cdc connector state: %v", err))
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return errors.NewConflictError(fmt.Sprintf("cdc connector state update conflict or not found: %s (v%d)", connectorName, version))
	}
	return nil
}

func (a *PostgresCDCRepositoryAdapter) UpdateWALLag(ctx context.Context, connectorName string, retainedBytes int64, lagBytes int64) error {
	_, err := a.db.ExecContext(ctx, queries.UpdateConnectorWALLag, retainedBytes, lagBytes, connectorName)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to update cdc connector wal lag: %v", err))
	}
	return nil
}

func (a *PostgresCDCRepositoryAdapter) RecordSignal(ctx context.Context, signal contracts.SignalRecord) error {
	payloadJSON, err := json.Marshal(signal.Payload)
	if err != nil {
		payloadJSON = []byte("{}")
	}

	_, execErr := a.db.ExecContext(ctx, queries.InsertSignalLog,
		signal.ID, signal.PublicID, signal.TenantID, signal.SourceService, signal.ExternalID, signal.CorrelationID,
		signal.ConnectorName, string(signal.SignalType), payloadJSON,
	)
	if execErr != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to insert cdc signal: %v", execErr))
	}
	return nil
}

func (a *PostgresCDCRepositoryAdapter) MarkSignalProcessed(ctx context.Context, signalID string) error {
	_, err := a.db.ExecContext(ctx, queries.MarkSignalProcessed, signalID)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to mark cdc signal processed: %v", err))
	}
	return nil
}

func (a *PostgresCDCRepositoryAdapter) RecordHeartbeat(ctx context.Context, input contracts.HeartbeatTickInput) error {
	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("hb_%s", strings.ReplaceAll(id, "-", "")[:20])

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to begin heartbeat transaction: %v", err))
	}
	defer tx.Rollback()

	_, execErr := tx.ExecContext(ctx, queries.InsertHeartbeatLog,
		id, publicID, input.TenantID, input.SourceService, input.ExternalID, input.CorrelationID,
		input.ConnectorName, input.LSNPosition,
	)
	if execErr != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to insert heartbeat log: %v", execErr))
	}

	_, updateErr := tx.ExecContext(ctx, queries.UpdateLastHeartbeat, input.ConnectorName)
	if updateErr != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to update connector heartbeat: %v", updateErr))
	}

	return tx.Commit()
}

func (a *PostgresCDCRepositoryAdapter) RecordCanarySent(ctx context.Context, input contracts.CanaryCheckInput) error {
	id, _ := utils.GenerateUUIDv7()
	publicID := fmt.Sprintf("cnr_%s", strings.ReplaceAll(id, "-", "")[:20])

	_, err := a.db.ExecContext(ctx, queries.InsertCanarySent,
		id, publicID, input.TenantID, input.SourceService, input.ExternalID, input.CorrelationID,
		input.ConnectorName, input.CanaryEventID,
	)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to insert canary sent: %v", err))
	}
	return nil
}

func (a *PostgresCDCRepositoryAdapter) RecordCanaryReceived(ctx context.Context, canaryEventID string, latencyMs int64) error {
	_, err := a.db.ExecContext(ctx, queries.UpdateCanaryReceived, latencyMs, canaryEventID)
	if err != nil {
		return errors.NewInternalError(fmt.Sprintf("failed to update canary received: %v", err))
	}
	return nil
}

