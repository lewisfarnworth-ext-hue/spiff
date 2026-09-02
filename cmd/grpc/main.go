package main

import (
	"log"
	"net"
	"os"

	"github.com/cloudwego/kitex/server"

	// Registers gzip so the server can decode requests from generic gRPC
	// clients (e.g. grpcurl) that advertise gzip content-coding by default.
	_ "github.com/cloudwego/kitex/pkg/remote/codec/protobuf/encoding/gzip"

	"spiff/internal/grpcserver"
	"spiff/internal/grpcserver/kitex_gen/spiffpb/spiff"
	"spiff/internal/netfetch"
	spifffs "spiff/internal/spiff_fs"
)

func main() {
	F := spifffs.New("data")

	if err := os.MkdirAll(F.DataDir(), 0o755); err != nil {
		log.Fatal(err)
	}

	addr, err := net.ResolveTCPAddr("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}

	svr := spiff.NewServer(grpcserver.NewHandler(F, netfetch.New()), server.WithServiceAddr(addr))

	log.Println("listening on :9090")
	if err := svr.Run(); err != nil {
		log.Fatal(err)
	}
}
