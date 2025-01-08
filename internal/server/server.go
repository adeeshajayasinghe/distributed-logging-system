package server

import (
	"context"

	api "github.com/adeeshajayasinghe/distributed-logging-system/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Config struct {
	CommitLog CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction = "produce"
	consumeAction = "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

// grpc server that listen for incoming connections
// i.e. Register our server
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {

	// Configure logging
	logger := zap.L().Named("server") // Specify logger's name to differentiate server logs from other logs
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func (duration time.Duration) zapcore.Field  {
				// Define our structured logs to log the duration of each request in nanoseconds
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}

	// Configure open-census collects metrics and traces
	// Configure to always sample the traces
	// This might trace confidential data and also affect the performance (not good for production)
	// Hence we could use a probability sampler and sample a percentage of the requests
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()}) 
	// Specify what stats will be traced. Default traces received bytes per RPC, Sent bytes per RPC, latency and completed RPCs
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	// Add the authenticate middleware to the gRPC server so that it identifies the subject
	// Middlewares for:
	// - Authenticate subject
	// - Zap interceptor that logs the gRPC calls
	// - attach OpenCensus as the server’s stat handler so that OpenCensus can record stats on the server’s request handling
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			grpc_auth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_ctxtags.UnaryServerInterceptor(),	
		grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		grpc_auth.UnaryServerInterceptor(authenticate),
	)),
	grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// Check for the authorization
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// Check for the authorization
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// Get a stream of requests and sends the response as a stream
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// Sends the response as a stream
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <- stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			// waits someone appends to the log
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// By creating a interface to Log libraries we can give any version of log based on the use case
// For example for the testing we could use native in-memory log
// While for production we can use log with a persistent memory
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

// We make the authorizer as the inteface so that we can switch out the authorization implementation in the future
type Authorizer interface {
	Authorize(subject, object, action string) error
}

// Helper function for taking out the subject out of the client's cert and writes it to the RPC's context
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlfInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlfInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// Returns the client cert's subject from the context
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

