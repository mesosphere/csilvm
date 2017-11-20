package csilvm

import (
	"context"

	"google.golang.org/grpc"
)

func loggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		log.Printf("Serving %v: req=%v", info.FullMethod, req)
		v, err := handler(ctx, req)
		if err != nil {
			log.Printf("%v failed: err=%v", info.FullMethod, err)
			return nil, err
		}
		log.Printf("Served %v: resp=%v", info.FullMethod, v)
		return v, nil
	}
}
