// http server to write and read from the log
package server

import (
	"encoding/json"
	"net/http"

	v1 "github.com/adityavit/proglog/api/v1"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/encoding/protojson"
)

func NewHTTPServer(addr string) *http.Server {
	httpServer := newHTTPServer()
	server := &http.Server{Addr: addr}
	router := mux.NewRouter()
	router.HandleFunc("/", httpServer.handleProduce).Methods("POST")
	router.HandleFunc("/", httpServer.handleConsume).Methods("GET")
	server.Handler = router
	return server
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// handleProduce is a handler to append a record to the log
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req v1.ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record := &v1.Record{
		Value: req.Record.Value,
	}
	offset, err := s.Log.Append(record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := v1.ProduceResponse{Offset: offset}
	w.Header().Set("Content-Type", "application/json")

	// Marshal using protojson to ensure defaults are included
	jsonBytes, err := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}.Marshal(&res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(jsonBytes)
}

// handleConsume is a handler to read a record from the log
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req v1.ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	res := v1.ConsumeResponse{Record: record}
	w.Header().Set("Content-Type", "application/json")
	jsonBytes, err := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}.Marshal(&res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(jsonBytes)
}
