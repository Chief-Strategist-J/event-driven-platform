package main

import (
	"net/http"
	"os"

	"event-driven-platform/messaging/engine/producers"
	"event-driven-platform/src/api/rest/v1/handlers"
	"event-driven-platform/src/api/rest/v1/routers"
	schemaAdapters "event-driven-platform/src/features/event-schema-registry/infrastructure/adapters"
	schemaService "event-driven-platform/src/features/event-schema-registry/service"
	outboxAdapters "event-driven-platform/src/features/transactional-outbox/infrastructure/adapters"
	outboxService "event-driven-platform/src/features/transactional-outbox/service"
	"event-driven-platform/src/infra/chassis"
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
			Database: false,
			Cache:    false,
			Broker:   false,
		},
		DependsOn: []chassis.DownstreamDependency{},
		Contracts: chassis.Contracts{
			APISchemas:     []string{"src/api/contracts/openapi/v1.yaml"},
			ProducedTopics: []string{},
			ConsumedTopics: []string{},
		},
		BuildServices: func(deps chassis.Dependencies) (chassis.Services, error) {
			schemaRepo := schemaAdapters.NewMemorySchemaRegistryAdapter()
			schemaSvc := schemaService.NewSchemaRegistryService(schemaRepo)

			outboxRepo := outboxAdapters.NewMemoryOutboxRepositoryAdapter()
			producer := producers.NewMemoryProducerAdapter()
			outboxSvc := outboxService.NewOutboxService(outboxRepo, producer)

			return chassis.Services{
				Registry: map[string]any{
					"schemaService": schemaSvc,
					"outboxService": outboxSvc,
				},
			}, nil
		},
		RegisterAPIRoutes: func(router *http.ServeMux, services chassis.Services) {
			instances, ok := services.Registry.(map[string]any)
			if !ok {
				return
			}
			if schemaSvc, exists := instances["schemaService"].(*schemaService.SchemaRegistryService); exists && schemaSvc != nil {
				schemaHandler := handlers.NewSchemaRegistryHandler(schemaSvc)
				schemaRouter := routers.NewSchemaRegistryRouter(schemaHandler)
				schemaRouter.RegisterRoutes(router)
			}
			if outboxSvc, exists := instances["outboxService"].(*outboxService.OutboxService); exists && outboxSvc != nil {
				outboxHandler := handlers.NewTransactionalOutboxHandler(outboxSvc)
				outboxRouter := routers.NewTransactionalOutboxRouter(outboxHandler)
				outboxRouter.RegisterRoutes(router)
			}
		},
	}
}
