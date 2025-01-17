package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/adeeshajayasinghe/distributed-logging-system/api/v1"
)

type Resolver struct {
	mu sync.Mutex
	// user's client connection and gRPC passes it to the resolver for the resolver to update with servers it discovers
	clientConn resolver.ClientConn
	// Resolver's own client connection to the server
	// so it can call GetServers() and get the servers
	resolverConn *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

// Build the custom resolver
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	// Set the resolver connection to local server so the resolver cam call the GetServers() API
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	// Resolve the servers and updates the client connection with the discovered servers
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "dls"

// Use to passes out from the target address and tries to find the resolver that matches
func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	// Since resolver may use this within goroutines we need to use a lock
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}
	// Servers that clients connect
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}
	// Update the user's connection from the servers discovered
	r.clientConn.UpdateState(resolver.State{
		Addresses: addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}