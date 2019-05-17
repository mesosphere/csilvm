package csilvm

import (
	"context"
	"sync"
	"time"

	"github.com/mesosphere/csilvm/pkg/lvm"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
)

const (
	resultTypeSuccess = "success"
	resultTypeError   = "error"
)

func MetricsInterceptor(scope tally.Scope) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		scope = scope.Tagged(map[string]string{
			"method": info.FullMethod,
		})
		timer := scope.SubScope("requests").Timer("latency")
		defer timer.Start().Stop()
		v, err := handler(ctx, req)
		if err != nil {
			scope.Tagged(map[string]string{"result_type": resultTypeError}).Counter("requests").Inc(1)
			return nil, err
		}
		scope.Tagged(map[string]string{"result_type": resultTypeSuccess}).Counter("requests").Inc(1)
		return v, nil
	}
}

func (s *Server) ReportUptime() context.CancelFunc {
	var wg sync.WaitGroup
	// Report uptime
	wg.Add(1)
	done := make(chan struct{})
	uptimeTicker := time.NewTicker(time.Second)
	defer uptimeTicker.Stop()
	go func() {
		defer wg.Done()
		gauge := s.metrics.Gauge("uptime")
		start := time.Now()
		for {
			select {
			case <-uptimeTicker.C:
				elapsed := time.Now().Sub(start)
				gauge.Update(float64(elapsed.Seconds()))
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// reportStorageMetrics sets various metrics gauges. It performs LVM2 CLI commands and
// is considered a somewhat costly operation. To avoid concurrent LVM2
// operations (specifically lvs concurrent with lvcreate) triggering latent
// issues we've run into this should probably not be called concurrently with
// other RPCs.
func (s *Server) reportStorageMetrics() {
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
