package adapters

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"event-driven-platform/src/features/encryption/contracts"
	"event-driven-platform/src/features/encryption/infrastructure/ports"
	"event-driven-platform/src/features/encryption/infrastructure/queries"
)

type PostgresEncryptionRepositoryAdapter struct {
	db     *sql.DB
	tracer trace.Tracer
}

func NewPostgresEncryptionRepositoryAdapter(db *sql.DB) ports.EncryptionRepositoryPort {
	return &PostgresEncryptionRepositoryAdapter{
		db:     db,
		tracer: otel.Tracer("PostgresEncryptionRepositoryAdapter"),
	}
}

func NewPostgresEncryptionRepository(db *sql.DB) ports.EncryptionRepositoryPort {
	return NewPostgresEncryptionRepositoryAdapter(db)
}

type SimulatedKMSClient struct {
	MasterKey []byte
}

func NewSimulatedKMSClient(key []byte) *SimulatedKMSClient {
	if len(key) == 0 {
		key = []byte("simulated-master-key-32-bytes-long!")
	}
	return &SimulatedKMSClient{MasterKey: key}
}

func (a *PostgresEncryptionRepositoryAdapter) GetActiveKEK(ctx context.Context, tenantID string) (*contracts.KEKRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresEncryptionRepositoryAdapter.GetActiveKEK")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.GetActiveKEKQuery, tenantID)
	var rec contracts.KEKRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.KeyName, &rec.KeyVersion,
		&rec.KMSProvider, &rec.KMSKeyARN, &rec.State, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &rec, nil
}

func (a *PostgresEncryptionRepositoryAdapter) RegisterKEK(ctx context.Context, kek contracts.KEKRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresEncryptionRepositoryAdapter.RegisterKEK")
	defer span.End()

	_, err := a.db.ExecContext(
		ctx,
		queries.InsertKEKQuery,
		kek.ID, kek.PublicID, kek.TenantID, kek.KeyName, kek.KeyVersion,
		kek.KMSProvider, kek.KMSKeyARN, kek.State, kek.CreatedAt, kek.UpdatedAt,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (a *PostgresEncryptionRepositoryAdapter) GetDEKBySubject(ctx context.Context, tenantID string, subjectID string) (*contracts.DEKRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresEncryptionRepositoryAdapter.GetDEKBySubject")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.GetDEKBySubjectQuery, tenantID, subjectID)
	var rec contracts.DEKRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SubjectID, &rec.KEKPublicID,
		&rec.EncryptedDEKBlob, &rec.Algorithm, &rec.State, &rec.CreatedAt,
		&rec.RotatedAt, &rec.ShreddedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &rec, nil
}

func (a *PostgresEncryptionRepositoryAdapter) GetDEKByPublicID(ctx context.Context, tenantID string, publicID string) (*contracts.DEKRecord, error) {
	ctx, span := a.tracer.Start(ctx, "PostgresEncryptionRepositoryAdapter.GetDEKByPublicID")
	defer span.End()

	row := a.db.QueryRowContext(ctx, queries.GetDEKByPublicIDQuery, tenantID, publicID)
	var rec contracts.DEKRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.SubjectID, &rec.KEKPublicID,
		&rec.EncryptedDEKBlob, &rec.Algorithm, &rec.State, &rec.CreatedAt,
		&rec.RotatedAt, &rec.ShreddedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return &rec, nil
}

func (a *PostgresEncryptionRepositoryAdapter) RegisterDEK(ctx context.Context, dek contracts.DEKRecord) error {
	ctx, span := a.tracer.Start(ctx, "PostgresEncryptionRepositoryAdapter.RegisterDEK")
	defer span.End()

	_, err := a.db.ExecContext(
		ctx,
		queries.InsertDEKQuery,
		dek.ID, dek.PublicID, dek.TenantID, dek.SubjectID, dek.KEKPublicID,
		dek.EncryptedDEKBlob, dek.Algorithm, dek.State, dek.CreatedAt,
		dek.RotatedAt, dek.ShreddedAt,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

func (a *PostgresEncryptionRepositoryAdapter) ShredSubjectDEK(ctx context.Context, in contracts.ShredSubjectInput, dekPublicID string) error {
	ctx, span := a.tracer.Start(ctx, "PostgresEncryptionRepositoryAdapter.ShredSubjectDEK")
	defer span.End()

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, queries.ShredDEKQuery, in.TenantID, in.SubjectID); err != nil {
		span.RecordError(err)
		return err
	}

	auditID := uuid.New().String()
	pubID := fmt.Sprintf("csa_%s", strings.ReplaceAll(auditID, "-", "")[:20])

	if _, err := tx.ExecContext(
		ctx,
		queries.InsertShredAuditQuery,
		auditID, pubID, in.TenantID, in.SubjectID, dekPublicID, in.Reason, in.RequestedBy, time.Now().UTC(),
	); err != nil {
		span.RecordError(err)
		return err
	}

	return tx.Commit()
}
