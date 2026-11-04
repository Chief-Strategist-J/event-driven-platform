package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"event-driven-platform/messaging/engine/producers"
	kafkaProducer "event-driven-platform/messaging/engine/producers/kafka"
	"event-driven-platform/src/api/rest/v1/handlers"
	"event-driven-platform/src/api/rest/v1/routers"
	dlqAdapters "event-driven-platform/src/features/dead-letter-handling/infrastructure/adapters"
	dlqContracts "event-driven-platform/src/features/dead-letter-handling/contracts"
	dlqService "event-driven-platform/src/features/dead-letter-handling/service"
	schemaAdapters "event-driven-platform/src/features/event-schema-registry/infrastructure/adapters"
	schemaService "event-driven-platform/src/features/event-schema-registry/service"
	versioningAdapters "event-driven-platform/src/features/event-versioning/infrastructure/adapters"
	versioningService "event-driven-platform/src/features/event-versioning/service"
	consumerAdapters "event-driven-platform/src/features/idempotent-consumers/infrastructure/adapters"
	consumerService "event-driven-platform/src/features/idempotent-consumers/service"
	outboxAdapters "event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	outboxService "event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/infra/chassis"
	"event-driven-platform/src/infra/database"
)

func main() {
	os.Exit(chassis.Run(describeService(), os.Args[1:], os.Environ()))
}

func describeService() chassis.ServiceSpec {
	return chassis.ServiceSpec{
		Identity: chassis.Identity{
			Name: "event-driven-platform",
			Team: "platform-core",
			Tier: "tier-1",
		},
		Needs: chassis.Needs{
			Database: true,
			Cache:    false,
			Broker:   true,
		},
		DependsOn: []chassis.DownstreamDependency{},
		Contracts: chassis.Contracts{
			APISchemas:     []string{"src/api/contracts/openapi/v1.yaml"},
			ProducedTopics: []string{},
			ConsumedTopics: []string{},
		},
		Migrations: func() chassis.MigrationSet {
			return chassis.MigrationSet{
				Version: 2,
				Apply: func(ctx context.Context) error {
					dbURL := os.Getenv("DATABASE_URL")
					if dbURL == "" {
						return nil
					}
					db, err := sql.Open("postgres", dbURL)
					if err != nil {
						return err
					}
					defer db.Close()
					return database.RunMigrations(ctx, db, "database/migrations/sql")
				},
			}
		},
		BuildServices: func(deps chassis.Dependencies) (chassis.Services, error) {
			schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
			if schemaRegistryURL == "" {
				schemaRegistryURL = "http://127.0.0.1:8082/apis/registry/v2"
			}
			schemaRepo := schemaAdapters.NewApicurioSchemaRegistryAdapter(schemaRegistryURL)
			schemaSvc := schemaService.NewSchemaRegistryService(schemaRepo)

			dbURL := os.Getenv("DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@127.0.0.1:5432/event_store?sslmode=disable"
			}
			db, err := sql.Open("postgres", dbURL)
			if err != nil {
				return chassis.Services{}, fmt.Errorf("failed connecting to postgres: %w", err)
			}

			outboxRepo := outboxAdapters.NewPostgresOutboxRepositoryAdapter(db)
			consumerRepo := consumerAdapters.NewPostgresConsumerRepositoryAdapter(db)
			dlqRepo := dlqAdapters.NewPostgresDeadLetterRepositoryAdapter(db)
			versioningRepo := versioningAdapters.NewPostgresEventVersioningAdapter(db)

			var producer producers.ProducerPort
			kafkaBrokersEnv := os.Getenv("KAFKA_BROKERS")
			if kafkaBrokersEnv == "" {
				kafkaBrokersEnv = "127.0.0.1:9092"
			}
			brokers := strings.Split(kafkaBrokersEnv, ",")
			producer = kafkaProducer.NewKafkaProducerAdapter(kafkaProducer.DefaultKafkaProducerConfig(brokers))

			outboxSvc := outboxService.NewOutboxService(outboxRepo, producer)
			consumerSvc := consumerService.NewConsumerService(consumerRepo)
			dlqSvc := dlqService.NewDeadLetterService(dlqRepo)
			versioningSvc := versioningService.NewEventVersioningService(versioningRepo, nil)

			return chassis.Services{
				Registry: map[string]any{
					"schemaService":     schemaSvc,
					"outboxService":     outboxSvc,
					"consumerService":   consumerSvc,
					"dlqService":        dlqSvc,
					"versioningService": versioningSvc,
				},
			}, nil
		},
		RegisterAPIRoutes: func(router *http.ServeMux, services chassis.Services) {
			instances, ok := services.Registry.(map[string]any)
			if !ok {
				return
			}
			if schemaSvc, exists := instances["schemaService"].(*schemaService.SchemaRegistryService); exists && schemaSvc != nil {
				schemaHandler := handlers.NewEventSchemaRegistryHandler(schemaSvc)
				schemaRouter := routers.NewEventSchemaRegistryRouter(schemaHandler)
				schemaRouter.RegisterRoutes(router)
			}
			if outboxSvc, exists := instances["outboxService"].(*outboxService.OutboxService); exists && outboxSvc != nil {
				outboxHandler := handlers.NewTransactionalOutboxHandler(outboxSvc)
				outboxRouter := routers.NewTransactionalOutboxRouter(outboxHandler)
				outboxRouter.RegisterRoutes(router)
			}
			if consumerSvc, exists := instances["consumerService"].(*consumerService.ConsumerService); exists && consumerSvc != nil {
				consumerHandler := handlers.NewIdempotentConsumerHandler(consumerSvc)
				consumerRouter := routers.NewIdempotentConsumerRouter(consumerHandler)
				consumerRouter.RegisterRoutes(router)
			}
			if dlqSvc, exists := instances["dlqService"].(*dlqService.DeadLetterService); exists && dlqSvc != nil {
				defaultInvoker := func(_ context.Context, _ dlqContracts.DeadLetterQuarantineRecord) (bool, error) {
					return true, nil
				}
				dlqHandler := handlers.NewDeadLetterHandler(dlqSvc, defaultInvoker)
				dlqRouter := routers.NewDeadLetterRouter(dlqHandler)
				dlqRouter.RegisterRoutes(router)
			}
			if versioningSvc, exists := instances["versioningService"].(*versioningService.EventVersioningService); exists && versioningSvc != nil {
				versioningHandler := handlers.NewEventVersioningHandler(versioningSvc)
				versioningRouter := routers.NewEventVersioningRouter(versioningHandler)
				versioningRouter.RegisterRoutes(router)
			}
		},
		RegisterDaemons: func(lifecycle *chassis.Lifecycle, services chassis.Services) {
			instances, ok := services.Registry.(map[string]any)
			if !ok {
				return
			}
			if outboxSvc, exists := instances["outboxService"].(*outboxService.OutboxService); exists && outboxSvc != nil {
				lifecycle.AddComponent("outbox-poller-daemon", func(ctx context.Context) error {
					return outboxSvc.StartPollerDaemon(ctx, 2*time.Second, 50)
				}, func(_ context.Context) error {
					return nil
				})
			}
		},
	}
}
