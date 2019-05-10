package csilvm

import (
	"context"

	"github.com/uber-go/tally"
	"google.golang.org/grpc"
)

func MetricsInterceptor(scope tally.Scope) grpc.UnaryServerInterceptor {
	scope = scope.SubScope("requests")
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		scope = scope.Tagged(map[string]string{
			"method": info.FullMethod,
		})
		timer := scope.Timer("duration")
		defer timer.Start().Stop()
		scope.Counter("served").Inc(1)
		v, err := handler(ctx, req)
		if err != nil {
			scope.Counter("failure").Inc(1)
			return nil, err
		}
		scope.Counter("success").Inc(1)
		return v, nil
	}
}
