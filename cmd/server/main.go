package main

import (
	"log"

	"github.com/adeeshajayasinghe/distributed-logging-system/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}