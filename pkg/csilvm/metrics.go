package csilvm

import (
	"context"
	"sync"
	"time"

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

func ReportUptime(scope tally.Scope, tags map[string]string) context.CancelFunc {
	scope = scope.Tagged(tags)
	var wg sync.WaitGroup
	wg.Add(1)
	ticker := time.NewTicker(time.Second)
	go func() {
		defer wg.Done()
		gauge := scope.Gauge("uptime")
		start := time.Now()
		for range ticker.C {
			elapsed := time.Now().Sub(start)
			gauge.Update(float64(elapsed.Seconds()))
		}
	}()
	return func() {
		ticker.Stop()
		wg.Wait()
	}
}
