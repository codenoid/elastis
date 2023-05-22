package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/schollz/progressbar/v3"
	"github.com/vmihailenco/msgpack"
)

type Exporter interface {
	Encode(v interface{}) error
	Flush() error
	SetFields(fields []string) error
}

type EsCountResponse struct {
	Count int `json:"count"`
}

type EsSearchResponse struct {
	Hits struct {
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	ScrollID string `json:"_scroll_id"`
}

func exportData(es *elasticsearch.Client) {

	// Perform a count request to obtain the total number of documents
	countReq := esapi.CountRequest{
		Index: []string{*ES_INDEX},
	}
	countRes, err := countReq.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error performing count request: %s", err)
	}

	if countRes.IsError() {
		log.Fatalf("Count request failed: %s", countRes.String())
	}

	b, err := io.ReadAll(countRes.Body)
	if err != nil {
		log.Fatalf("Error reading the count response body: %s", err)
	}

	countRes.Body.Close()

	countResponse := EsCountResponse{}
	if err := json.Unmarshal(b, &countResponse); err != nil {
		log.Fatalf("Error decoding the count response body: %s", err)
	}

	totalCount := countResponse.Count

	// Create the output file
	file, err := os.Create(*OUTPUT)
	if err != nil {
		log.Fatalf("Error creating file: %s", err)
	}
	defer file.Close()

	// Create a buffered writer to improve performance
	bufferedWriter := bufio.NewWriter(file)
	defer bufferedWriter.Flush()

	var exporter Exporter
	// Initialize the appropriate exporter
	switch *FILE_FORMAT {
	case "msgpack":
		exporter = &MsgPackExporter{
			encoder: msgpack.NewEncoder(bufferedWriter),
		}
	case "csv":
		exporter = &CSVExporter{
			writer: csv.NewWriter(bufferedWriter),
		}
	default:
		log.Fatalf("Invalid export format: %s", *FILE_FORMAT)
	}

	batch := *ES_BATCH_SIZE

	// Create a progress bar with the maximum value set to the total count
	ctx := context.Background()
	pb := progressbar.Default(int64(totalCount), "Exporting")

	// Specify the pagination parameters
	size := batch // Number of documents to retrieve in each batch

	// Initialize the scroll
	scrollTime := 1 * time.Minute // Keep the scroll context alive for 1 minute

	// Create an initial search request with the scroll parameter
	initialReq := esapi.SearchRequest{
		Index:  []string{*ES_INDEX},
		Scroll: scrollTime,
		Size:   &size,
	}

	// Perform the initial search request
	initialRes, err := initialReq.Do(ctx, es)
	if err != nil {
		log.Fatalf("Error performing initial search request: %s", err)
	}

	if initialRes.IsError() {
		log.Fatalf("Initial search request failed: %s", initialRes.String())
	}

	// Read and decode the response body
	b, err = io.ReadAll(initialRes.Body)
	if err != nil {
		log.Fatalf("Error reading the initial search response body: %s", err)
	}

	initialRes.Body.Close()

	// Decode the response body
	searchResponse := EsSearchResponse{}
	if err := json.Unmarshal(b, &searchResponse); err != nil {
		log.Fatalf("Error decoding the initial search response body: %s", err)
	}

	// Get the scroll ID
	scrollID := searchResponse.ScrollID

	fields := make([]string, 0)
	// Process the hits from the initial search request
	for _, hit := range searchResponse.Hits.Hits {
		if len(fields) == 0 {
			for key := range hit.Source {
				fields = append(fields, key)
			}
			exporter.SetFields(fields)
		}
		err = exporter.Encode(hit.Source)
		if err != nil {
			log.Fatalf("Error encoding hit: %s", err)
		}
	}

	pb.Add(len(searchResponse.Hits.Hits))

	for {

		// Create a new scroll request
		scrollReq := esapi.ScrollRequest{
			ScrollID: scrollID,
			Scroll:   scrollTime,
		}

		// Perform the scroll request
		scrollRes, err := scrollReq.Do(ctx, es)
		if err != nil {
			log.Fatalf("Error performing scroll request: %s", err)
		}

		if scrollRes.IsError() {
			log.Fatalf("Scroll request failed: %s", scrollRes.String())
		}

		// Read and decode the response body
		b, err = io.ReadAll(scrollRes.Body)
		if err != nil {
			log.Fatalf("Error reading the scroll response body: %s", err)
		}

		scrollRes.Body.Close()

		// Decode the response body
		searchResponse := EsSearchResponse{}
		if err := json.Unmarshal(b, &searchResponse); err != nil {
			log.Fatalf("Error decoding the scroll response body: %s", err)
		}

		// Process the hits
		for _, hit := range searchResponse.Hits.Hits {
			err = exporter.Encode(hit.Source)
			if err != nil {
				log.Fatalf("Error encoding hit: %s", err)
			}
		}

		// Flush the exporter
		err = exporter.Flush()
		if err != nil {
			log.Fatalf("Error flushing exporter: %s", err)
		}

		pb.Add(len(searchResponse.Hits.Hits))

		// Check if all documents have been retrieved
		if len(searchResponse.Hits.Hits) == 0 {
			break
		}

	}

	fmt.Println("Done, saved to", *OUTPUT)
}
