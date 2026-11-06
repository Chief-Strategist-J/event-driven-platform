package cqrs

import (
	"database/sql"

	"event-driven-platform/src/features/cqrs/contracts"
	"event-driven-platform/src/features/cqrs/infrastructure/adapters"
	"event-driven-platform/src/features/cqrs/infrastructure/ports"
	"event-driven-platform/src/features/cqrs/service"
)

type CQRSService = service.CQRSService
type CQRSRepositoryPort = ports.CQRSRepositoryPort

type CommandRecord = contracts.CommandRecord
type AggregateRecord = contracts.AggregateRecord
type TokenSigningKeyRecord = contracts.TokenSigningKeyRecord
type ConsistencyToken = contracts.ConsistencyToken

type DispatchCommandInput = contracts.DispatchCommandInput
type DispatchCommandOutput = contracts.DispatchCommandOutput
type ResolveQueryInput = contracts.ResolveQueryInput
type ResolveQueryOutput = contracts.ResolveQueryOutput
type RegisterSigningKeyInput = contracts.RegisterSigningKeyInput
type AuthoritativeReadInput = contracts.AuthoritativeReadInput

func NewCQRSService(repo ports.CQRSRepositoryPort) *service.CQRSService {
	return service.NewCQRSService(repo)
}

func NewService(db *sql.DB) *service.CQRSService {
	repo := adapters.NewPostgresCQRSRepositoryAdapter(db)
	return service.NewCQRSService(repo)
}

func NewPostgresCQRSRepositoryAdapter(db *sql.DB) ports.CQRSRepositoryPort {
	return adapters.NewPostgresCQRSRepositoryAdapter(db)
}

