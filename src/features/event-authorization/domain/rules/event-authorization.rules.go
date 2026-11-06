package rules

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"event-driven-platform/src/features/event-authorization/contracts"
)

var UntrustedIdentityHeaders = []string{
	"X-User-ID",
	"X-User-Id",
	"x-user-id",
	"X-Roles",
	"x-roles",
	"X-Tenant-ID",
	"X-Tenant-Id",
	"x-tenant-id",
	"X-Forwarded-Identity",
	"x-forwarded-identity",
}

func SanitizeInboundHeaders(headers http.Header) {
	for _, h := range UntrustedIdentityHeaders {
		headers.Del(h)
	}
}

func MatchResourcePattern(pattern, resourceName string) bool {
	pattern = strings.TrimSpace(pattern)
	resourceName = strings.TrimSpace(resourceName)
	if pattern == "*" || pattern == resourceName {
		return true
	}
	matched, err := filepath.Match(pattern, resourceName)
	if err == nil && matched {
		return true
	}
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		return strings.HasPrefix(resourceName, prefix+".") || resourceName == prefix
	}
	return false
}

func MatchOperation(policyOp, requestedOp contracts.Operation) bool {
	if policyOp == contracts.OpAll {
		return true
	}
	return policyOp == requestedOp
}

func ValidateTokenAudience(tokenAudience, targetService string) error {
	tokenAudience = strings.TrimSpace(tokenAudience)
	targetService = strings.TrimSpace(targetService)
	if tokenAudience == "" || targetService == "" {
		return contracts.ErrAudienceMismatch
	}
	if tokenAudience != targetService {
		return fmt.Errorf("%w: expected %s, got %s", contracts.ErrAudienceMismatch, targetService, tokenAudience)
	}
	return nil
}

func EvaluatePolicies(policies []*contracts.AuthPolicyRecord, resourceName string, requestedOp contracts.Operation) (contracts.Decision, string, string) {
	var matchedAllowPolicy string
	for _, pol := range policies {
		if pol.State != contracts.PolicyStateActive {
			continue
		}
		if !MatchResourcePattern(pol.ResourcePattern, resourceName) {
			continue
		}
		if !MatchOperation(pol.Operation, requestedOp) {
			continue
		}
		if pol.Effect == contracts.EffectDeny {
			return contracts.DecisionDenied, pol.PublicID, fmt.Sprintf("explicit deny policy %s triggered", pol.PublicID)
		}
		if pol.Effect == contracts.EffectAllow && matchedAllowPolicy == "" {
			matchedAllowPolicy = pol.PublicID
		}
	}

	if matchedAllowPolicy != "" {
		return contracts.DecisionAllowed, matchedAllowPolicy, "explicit allow policy matched"
	}
	return contracts.DecisionDenied, "NONE", "default deny: no active matching allow policy"
}
