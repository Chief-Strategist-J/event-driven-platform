package main

import (
	"net/http"
	"os"

	"event-driven-platform/src/api/rest/v1/handlers"
	"event-driven-platform/src/api/rest/v1/routers"
	schemaAdapters "event-driven-platform/src/features/event-schema-registry/infrastructure/adapters"
	schemaService "event-driven-platform/src/features/event-schema-registry/service"
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
			APISchemas:     []string{"contracts/openapi/v1.yml"},
			ProducedTopics: []string{},
			ConsumedTopics: []string{},
		},
		BuildServices: func(deps chassis.Dependencies) (chassis.Services, error) {
			schemaRepo := schemaAdapters.NewMemorySchemaRegistryAdapter()
			schemaSvc := schemaService.NewSchemaRegistryService(schemaRepo)
			return chassis.Services{
				Registry: schemaSvc,
			}, nil
		},
		RegisterAPIRoutes: func(router *http.ServeMux, services chassis.Services) {
			schemaSvc, ok := services.Registry.(*schemaService.SchemaRegistryService)
			if !ok {
				return
			}
			schemaHandler := handlers.NewSchemaRegistryHandler(schemaSvc)
			schemaRouter := routers.NewSchemaRegistryRouter(schemaHandler)
			schemaRouter.RegisterRoutes(router)
		},
	}
}
