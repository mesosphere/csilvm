package csilvm

import (
	"context"
	stdlog "log"
	"os"

	"google.golang.org/grpc"
)

type logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

var log logger = stdlog.New(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)

func SetLogger(l logger) {
	log = l
}

func LoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		log.Printf("Serving %v: req=%v", info.FullMethod, req)
		v, err := handler(ctx, req)
		if err != nil {
			log.Printf("%v failed: err=%v", info.FullMethod, err)
			return v, err
		}
		log.Printf("Served %v: resp=%v", info.FullMethod, v)
		return v, nil
	}
}
