package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// model: path to the file which defines the Casbian's authorization mechanism
// policy: path to the CSV file containing ACL table
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	// Enforce decides whether a "subject" can access a "object" with the operation "action"
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}