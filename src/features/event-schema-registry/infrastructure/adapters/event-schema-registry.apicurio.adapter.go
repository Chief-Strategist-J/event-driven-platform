package adapters

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	registryContracts "event-driven-platform/src/features/event-schema-registry/contracts"
	"event-driven-platform/src/features/event-schema-registry/infrastructure/ports"
	"event-driven-platform/src/shared/errors"
)

// ApicurioSchemaRegistryAdapter communicates with the live Apicurio Registry v2 HTTP REST API
type ApicurioSchemaRegistryAdapter struct {
	baseURL    string
	httpClient *http.Client
}

func NewApicurioSchemaRegistryAdapter(baseURL string) ports.SchemaRegistryRepositoryPort {
	if baseURL == "" {
		baseURL = "http://127.0.0.1:8082/apis/registry/v2"
	}
	return &ApicurioSchemaRegistryAdapter{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (a *ApicurioSchemaRegistryAdapter) RegisterSchema(ctx context.Context, schema *registryContracts.SchemaDefinition) error {
	group := schema.TenantID
	artifactID := schema.ID

	url := fmt.Sprintf("%s/groups/%s/artifacts", a.baseURL, group)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(schema.Schema)))
	if err != nil {
		return fmt.Errorf("failed to create Apicurio request: %w", err)
	}

	req.Header.Set("X-Registry-ArtifactId", artifactID)
	req.Header.Set("X-Registry-ArtifactType", "JSON")
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to Apicurio Registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return errors.NewConflictError(fmt.Sprintf("schema %s already exists in Apicurio group %s", artifactID, group))
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("apicurio registration failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func (a *ApicurioSchemaRegistryAdapter) GetSchemaByID(ctx context.Context, tenantID string, id string) (*registryContracts.SchemaDefinition, error) {
	group := tenantID
	artifactID := id

	url := fmt.Sprintf("%s/groups/%s/artifacts/%s", a.baseURL, group, artifactID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("apicurio query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.NewNotFoundError(fmt.Sprintf("schema %s not found in group %s", artifactID, group))
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("apicurio fetch failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	contentBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading schema content: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)

	return &registryContracts.SchemaDefinition{
		ID:        artifactID,
		TenantID:  tenantID,
		Subject:   artifactID,
		Format:    "json",
		Schema:    string(contentBytes),
		Status:    registryContracts.SchemaStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}
