package routers

import (
	"net/http"

	"event-driven-platform/src/api/rest/v1/handlers"
)

type ConsumerGroupsRouter struct {
	handler *handlers.ConsumerGroupsHandler
}

func NewConsumerGroupsRouter(handler *handlers.ConsumerGroupsHandler) *ConsumerGroupsRouter {
	return &ConsumerGroupsRouter{handler: handler}
}

func (r *ConsumerGroupsRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/consumer-groups/members", r.handler.RegisterMember)
	mux.HandleFunc("POST /api/v1/consumer-groups/heartbeat", r.handler.Heartbeat)
	mux.HandleFunc("POST /api/v1/consumer-groups/rebalance", r.handler.RebalanceSync)
	mux.HandleFunc("GET /api/v1/consumer-groups/members", r.handler.ListMembers)
}
