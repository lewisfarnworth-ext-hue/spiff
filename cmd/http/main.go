package main

import (
	"log"
	"net/http"
	"os"
	"spiff/internal/httphandlers"
	"spiff/internal/netfetch"
	spifffs "spiff/internal/spiff_fs"
)

const filesRoutePrefix = "/files/"
const compareRoute = "/compare"
const fetchRoute = "/fetch"

func main() {
	F := spifffs.New("data")

	if err := os.MkdirAll(F.DataDir(), 0o755); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", httphandlers.NewUploadHandler(F))
	http.HandleFunc(filesRoutePrefix, httphandlers.NewDownloadHandler(F, filesRoutePrefix))
	http.HandleFunc(compareRoute, httphandlers.NewCompareHandler(F))
	http.HandleFunc(fetchRoute, httphandlers.NewFetchHandler(F, netfetch.New()))
	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
