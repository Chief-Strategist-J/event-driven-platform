package machines

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"event-driven-platform/src/features/event-versioning/contracts"
)

var (
	ErrTransformerNotFound = errors.New("transformer function not found in registry")
	ErrInvalidStepOrder     = errors.New("target version must be greater than source version")
)

type UpcastTransformerFunc func(payload map[string]any) (map[string]any, error)

type UpcasterEngine struct {
	mu           sync.RWMutex
	transformers map[string]UpcastTransformerFunc
}

func NewUpcasterEngine() *UpcasterEngine {
	return &UpcasterEngine{
		transformers: make(map[string]UpcastTransformerFunc),
	}
}

func (e *UpcasterEngine) RegisterTransformer(name string, fn UpcastTransformerFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.transformers[name] = fn
}

func (e *UpcasterEngine) ExecuteChain(
	input contracts.UpcastStepInput,
	chain []contracts.UpcasterRecord,
) (*contracts.UpcastResult, error) {
	if input.TargetVersion <= input.SourceVersion {
		return &contracts.UpcastResult{
			EventType:      input.EventType,
			InitialVersion: input.SourceVersion,
			FinalVersion:   input.SourceVersion,
			Payload:        input.Payload,
			StepsApplied:   0,
		}, nil
	}

	var currentMap map[string]any
	if err := json.Unmarshal(input.Payload, &currentMap); err != nil {
		return nil, fmt.Errorf("invalid json payload for upcasting: %w", err)
	}

	currentVersion := input.SourceVersion
	stepsApplied := 0

	for _, step := range chain {
		if !step.IsActive {
			continue
		}
		if step.FromVersion != currentVersion {
			continue
		}

		e.mu.RLock()
		transformer, exists := e.transformers[step.TransformerName]
		e.mu.RUnlock()

		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrTransformerNotFound, step.TransformerName)
		}

		transformed, err := transformer(currentMap)
		if err != nil {
			return nil, fmt.Errorf("transformer %s failed on version %d: %w", step.TransformerName, currentVersion, err)
		}

		currentMap = transformed
		currentVersion = step.ToVersion
		stepsApplied++

		if currentVersion >= input.TargetVersion {
			break
		}
	}

	if currentVersion < input.TargetVersion {
		return nil, fmt.Errorf("incomplete upcast chain: reached version %d but requested %d", currentVersion, input.TargetVersion)
	}

	outBytes, err := json.Marshal(currentMap)
	if err != nil {
		return nil, fmt.Errorf("failed serializing upcasted payload: %w", err)
	}

	return &contracts.UpcastResult{
		EventType:      input.EventType,
		InitialVersion: input.SourceVersion,
		FinalVersion:   currentVersion,
		Payload:        outBytes,
		StepsApplied:   stepsApplied,
	}, nil
}

