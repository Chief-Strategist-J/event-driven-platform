package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
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
	orderingAdapters "event-driven-platform/src/features/event-ordering-protection/infrastructure/adapters"
	orderingService "event-driven-platform/src/features/event-ordering-protection/service"
	retryAdapters "event-driven-platform/src/features/retry-topics/infrastructure/adapters"
	retryService "event-driven-platform/src/features/retry-topics/service"
	groupAdapters "event-driven-platform/src/features/consumer-groups/infrastructure/adapters"
	groupService "event-driven-platform/src/features/consumer-groups/service"
	offsetAdapters "event-driven-platform/src/features/offset-management/infrastructure/adapters"
	offsetPorts "event-driven-platform/src/features/offset-management/infrastructure/ports"
	offsetService "event-driven-platform/src/features/offset-management/service"
	bpAdapters "event-driven-platform/src/features/backpressure/infrastructure/adapters"
	bpPorts "event-driven-platform/src/features/backpressure/infrastructure/ports"
	bpService "event-driven-platform/src/features/backpressure/service"
	replayAdapters "event-driven-platform/src/features/replay-mechanism/infrastructure/adapters"
	replayPorts "event-driven-platform/src/features/replay-mechanism/infrastructure/ports"
	replayService "event-driven-platform/src/features/replay-mechanism/service"
	consumerAdapters "event-driven-platform/src/features/idempotent-consumers/infrastructure/adapters"
	consumerService "event-driven-platform/src/features/idempotent-consumers/service"
	outboxAdapters "event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	outboxService "event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/infra/chassis"
	"event-driven-platform/src/infra/config"
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
			ProducedTopics: []string{
				"platform.events.outbox",
				"platform.events.retry",
				"platform.events.dlq",
			},
			ConsumedTopics: []string{
				"platform.events.ingest",
				"platform.events.retry",
			},
		},
		Migrations: func() chassis.MigrationSet {
			return chassis.MigrationSet{
				Version: 11,
				Apply: func(ctx context.Context) error {
					cfg, err := config.LoadConfig("")
					if err != nil {
						return err
					}
					db, err := sql.Open("postgres", cfg.Database.URL)
					if err != nil {
						return err
					}
					defer db.Close()
					return database.RunMigrations(ctx, db, "database/migrations/sql")
				},
			}
		},
		BuildServices: func(deps chassis.Dependencies) (chassis.Services, error) {
			cfg, err := config.LoadConfig("")
			if err != nil {
				return chassis.Services{}, fmt.Errorf("failed to load validated config: %w", err)
			}

			schemaRepo := schemaAdapters.NewApicurioSchemaRegistryAdapter(cfg.SchemaRegistry.URL)
			schemaSvc := schemaService.NewSchemaRegistryService(schemaRepo)

			db, err := sql.Open("postgres", cfg.Database.URL)
			if err != nil {
				return chassis.Services{}, fmt.Errorf("failed connecting to postgres: %w", err)
			}

			outboxRepo := outboxAdapters.NewPostgresOutboxRepositoryAdapter(db)
			consumerRepo := consumerAdapters.NewPostgresConsumerRepositoryAdapter(db)
			dlqRepo := dlqAdapters.NewPostgresDeadLetterRepositoryAdapter(db)
			versioningRepo := versioningAdapters.NewPostgresEventVersioningAdapter(db)
			orderingRepo := orderingAdapters.NewPostgresEventOrderingAdapter(db)
			retryRepo := retryAdapters.NewPostgresRetryTopicsAdapter(db)
			groupRepo := groupAdapters.NewPostgresConsumerGroupsAdapter(db)

			var producer producers.ProducerPort
			brokers := config.GetBrokerList(cfg.Broker.KafkaBrokers)
			producer = kafkaProducer.NewKafkaProducerAdapter(kafkaProducer.DefaultKafkaProducerConfig(brokers))

			outboxSvc := outboxService.NewOutboxService(outboxRepo, producer)
			consumerSvc := consumerService.NewConsumerService(consumerRepo)
			dlqSvc := dlqService.NewDeadLetterService(dlqRepo)
			versioningSvc := versioningService.NewEventVersioningService(versioningRepo, nil)
			orderingSvc := orderingService.NewEventOrderingService(orderingRepo)
			retrySvc := retryService.NewRetryTopicsService(retryRepo)
			groupSvc := groupService.NewConsumerGroupsService(groupRepo)
			offsetRepo := offsetAdapters.NewPostgresOffsetManagementAdapter(db)
			offsetSvc := offsetService.NewOffsetManagementService(offsetRepo)
			bpRepo := bpAdapters.NewBackpressurePostgresRepositoryAdapter(db)
			bpSvc := bpService.NewBackpressureService(bpRepo)
			replayRepo := replayAdapters.NewReplayPostgresRepositoryAdapter(db)
			replaySvc := replayService.NewReplayService(replayRepo)

			return chassis.Services{
				Registry: map[string]any{
					"db":                db,
					"schemaService":     schemaSvc,
					"outboxService":     outboxSvc,
					"consumerService":   consumerSvc,
					"dlqService":        dlqSvc,
					"versioningService": versioningSvc,
					"orderingService":   orderingSvc,
					"retryService":      retrySvc,
					"groupService":      groupSvc,
					"offsetRepo":        offsetRepo,
					"offsetService":     offsetSvc,
					"bpRepo":            bpRepo,
					"bpService":         bpSvc,
					"replayRepo":        replayRepo,
					"replayService":     replaySvc,
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
			if orderingSvc, exists := instances["orderingService"].(*orderingService.EventOrderingService); exists && orderingSvc != nil {
				orderingHandler := handlers.NewEventOrderingHandler(orderingSvc)
				orderingRouter := routers.NewEventOrderingRouter(orderingHandler)
				orderingRouter.RegisterRoutes(router)
			}
			if retrySvc, exists := instances["retryService"].(*retryService.RetryTopicsService); exists && retrySvc != nil {
				retryHandler := handlers.NewRetryTopicsHandler(retrySvc)
				retryRouter := routers.NewRetryTopicsRouter(retryHandler)
				retryRouter.RegisterRoutes(router)
			}
			if groupSvc, exists := instances["groupService"].(*groupService.ConsumerGroupsService); exists && groupSvc != nil {
				groupHandler := handlers.NewConsumerGroupsHandler(groupSvc)
				groupRouter := routers.NewConsumerGroupsRouter(groupHandler)
				groupRouter.RegisterRoutes(router)
			}
			if offsetRepo, exists := instances["offsetRepo"].(offsetPorts.OffsetManagementRepositoryPort); exists && offsetRepo != nil {
				routers.RegisterOffsetManagementRoutes(router, offsetRepo)
			}
			if bpRepo, exists := instances["bpRepo"].(bpPorts.BackpressureRepositoryPort); exists && bpRepo != nil {
				routers.RegisterBackpressureRoutes(router, bpRepo)
			}
			if replayRepo, exists := instances["replayRepo"].(replayPorts.ReplayRepositoryPort); exists && replayRepo != nil {
				routers.RegisterReplayRoutes(router, replayRepo)
			}
			if dbConn, exists := instances["db"].(*sql.DB); exists && dbConn != nil {
				routers.RegisterMultiTenantRoutes(router, dbConn)
				routers.RegisterCDCRoutes(router, dbConn)
				routers.RegisterPartitioningRoutes(router, dbConn)
			}
		},
		RegisterConsumers: func(_ any, _ chassis.Services) {},
		Jobs: func() []chassis.JobSpec {
			return []chassis.JobSpec{}
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
