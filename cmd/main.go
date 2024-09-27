package main

import (
	"github.com/adityavit/proglog/internal/server"
	"log"
)

// Run the server
func main() {
	server := server.NewHTTPServer(":8080")
	log.Fatal(server.ListenAndServe())
}
