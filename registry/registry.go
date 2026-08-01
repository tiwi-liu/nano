// Package registry defines service-discovery contracts without selecting a backend.
package registry

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInvalidMember  = errors.New("invalid registry member")
	ErrMemberNotFound = errors.New("registry member not found")
)

type MemberStatus string

const (
	MemberStatusActive   MemberStatus = "active"
	MemberStatusDraining MemberStatus = "draining"
	MemberStatusRetired  MemberStatus = "retired"
)

type EventType string

const (
	EventPut    EventType = "put"
	EventDelete EventType = "delete"
)

type Member struct {
	ID          string            `json:"id"`
	Label       string            `json:"label"`
	ServiceAddr string            `json:"serviceAddr"`
	ClientAddr  string            `json:"clientAddr,omitempty"`
	Services    []string          `json:"services"`
	Status      MemberStatus      `json:"status"`
	Revision    int64             `json:"revision,omitempty"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}
type Event struct {
	Type   EventType
	Member Member
}
type RegisterOptions struct{ TTL time.Duration }
type Lease interface {
	KeepAlive(context.Context) error
	Close(context.Context) error
}
type Registry interface {
	Register(context.Context, Member, RegisterOptions) (Lease, error)
	UpdateStatus(context.Context, string, MemberStatus) error
	Unregister(context.Context, string) error
	List(context.Context) ([]Member, error)
	Watch(context.Context, int64) (<-chan Event, <-chan error)
}

func NormalizeMember(member Member) (Member, error) {
	if member.ID == "" || member.ServiceAddr == "" || len(member.Services) == 0 {
		return Member{}, ErrInvalidMember
	}
	if member.Status == "" {
		member.Status = MemberStatusActive
	}
	if !ValidStatus(member.Status) {
		return Member{}, ErrInvalidMember
	}
	if member.UpdatedAt.IsZero() {
		member.UpdatedAt = time.Now().UTC()
	}
	member.Services = compactStrings(member.Services)
	if len(member.Services) == 0 {
		return Member{}, ErrInvalidMember
	}
	return member, nil
}
func ValidStatus(status MemberStatus) bool {
	return status == MemberStatusActive || status == MemberStatusDraining || status == MemberStatusRetired
}
func IsRoutable(status MemberStatus) bool { return status == MemberStatusActive }
func IsAddressable(status MemberStatus) bool {
	return status == MemberStatusActive || status == MemberStatusDraining
}
func compactStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
