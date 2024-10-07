package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/adityavit/proglog/internal/server"
)

// Run the server
func main() {
	//take logDir from arguments
	logDir := flag.String("logDir", "/tmp/proglog", "directory to store log files")
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()
	fmt.Printf("logDir: %s, addr: %s\n", *logDir, *addr)
	//create log directory
	err := os.MkdirAll(*logDir, 0o755)
	if err != nil {
		log.Fatal(err)
	}
	//create a new http server
	server, err := server.NewHTTPServer(*logDir, *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.ListenAndServe())
}
