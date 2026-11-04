package chassis

import (
	"context"
	"net/http"
)

type Identity struct {
	Name string
	Team string
	Tier string
}

type Needs struct {
	Database bool
	Cache    bool
	Broker   bool
}

type DownstreamDependency struct {
	ServiceName string
	Criticality string
}

type Contracts struct {
	APISchemas     []string
	ProducedTopics []string
	ConsumedTopics []string
}

type Dependencies struct {
	Context context.Context
}

type Services struct {
	Registry any
}

type JobSpec struct {
	Name    string
	Execute func(ctx context.Context) error
}

type MigrationSet struct {
	Version int
	Apply   func(ctx context.Context) error
}

type ServiceSpec struct {
	Identity          Identity
	Needs             Needs
	DependsOn         []DownstreamDependency
	Contracts         Contracts
	BuildServices     func(deps Dependencies) (Services, error)
	RegisterAPIRoutes func(router *http.ServeMux, services Services)
	RegisterConsumers func(group any, services Services)
	Jobs              func() []JobSpec
	Migrations        func() MigrationSet
}
