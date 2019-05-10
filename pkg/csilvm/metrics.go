package csilvm

import (
	"context"
	"sync"
	"time"

	"github.com/mesosphere/csilvm/pkg/lvm"
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

func (s *Server) ReportUptime() context.CancelFunc {
	var wg sync.WaitGroup
	// Report uptime
	wg.Add(1)
	uptimeTicker := time.NewTicker(time.Second)
	go func() {
		defer wg.Done()
		gauge := s.metrics.Gauge("uptime")
		start := time.Now()
		for range uptimeTicker.C {
			elapsed := time.Now().Sub(start)
			gauge.Update(float64(elapsed.Seconds()))
		}
	}()
	return func() {
		uptimeTicker.Stop()
		wg.Wait()
	}
}

func (s *Server) reportMetrics() {
	// Report the number of volumes
	volNames, err := s.volumeGroup.ListLogicalVolumeNames()
	if err != nil {
		log.Printf("failed to report metrics: cannot load lv names: err=%v", err)
		return
	}
	s.metrics.Gauge("volumes").Update(float64(len(volNames)))
	// Report the total bytes free for the volume group.
	bytesTotal, err := s.volumeGroup.BytesTotal()
	if err != nil {
		log.Printf("failed to report metrics: cannot read total bytes: err=%v", err)
		return
	}
	s.metrics.Gauge("bytes-total").Update(float64(bytesTotal))
	// Report the number of bytes free for the volume group.
	bytesFree, err := s.volumeGroup.BytesFree(lvm.VolumeLayout{
		Type: lvm.VolumeTypeLinear,
	})
	if err != nil {
		log.Printf("failed to report metrics: cannot read free bytes: err=%v", err)
		return
	}
	s.metrics.Gauge("bytes-free").Update(float64(bytesFree))
	// Report the number of bytes used.
	s.metrics.Gauge("bytes-used").Update(float64(bytesTotal - bytesFree))
}
