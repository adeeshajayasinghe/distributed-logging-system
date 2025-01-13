package log

import (
	"context"
	"sync"

	api "github.com/adeeshajayasinghe/distributed-logging-system/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	// Replicator connects to other servers with the gRPC client
	LocalServer api.LogClient

	logger *zap.Logger

	mu sync.Mutex
	// Map of server addresses to a channel, which replicator use to stop replicating from a server when the server fails or leaves the cluster
	servers map[string]chan struct{}
	closed bool
	close chan struct{}
}

// Adds the given server address to the list of servers
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		// already replicating so skip
		return nil
	}
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()
	// Create a client and open up a stream to consume all logs on the server
	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
			r.logError(err, "failed to consume", addr)
			return
	}
	records := make(chan *api.Record)
	go func ()  {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	// Replicate messages from the other server until that server fails or leaves the cluster and replicator close the channel for that server which breaks the loop
	// and ends the replicate() go routine
	for {
		select {
		case <- r.close:
			return
		case <- leave:
			return
		case record := <- records:
			_, err = r.LocalServer.Produce(ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// When the server receives a serf event that the server it connected left the cluster
// then it will call this Leave function and make the replicator to ends the go routine
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close the replicator so it doesn't replicate new servers that joins the cluster
// also it stops replicating from existing servers by causing the replicate() go routine to return 
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}