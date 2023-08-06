package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/schollz/progressbar/v3"
	"github.com/vmihailenco/msgpack"
)

type EsSearch map[string]interface{}

func importData(es *elasticsearch.Client) {
	if *CREATE_INDEX {
		// Create index before inserting data
		// default mapping are dynamic with no date or numeric validation
		createIndex(es, *ES_INDEX)
	}

	if *INPUT == "" {
		log.Fatalf("No input file provided")
	}

	// Open the input file
	file, err := os.Open(*INPUT)
	if err != nil {
		log.Fatalf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a buffered reader to improve performance
	bufferedReader := bufio.NewReader(file)

	// Create a progress bar
	pb := progressbar.Default(-1, "Importing")

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         *ES_INDEX,        // The default index name
		Client:        es,               // The Elasticsearch client
		NumWorkers:    runtime.NumCPU(), // The number of worker goroutines
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}

	// Count the documents indexed
	count := uint64(0)
	errCount := uint64(0)

	// Decide the import type
	if *FILE_FORMAT == "msgpack" {
		// Create a MessagePack decoder with the buffered reader as the source
		decoder := msgpack.NewDecoder(bufferedReader)

		for {
			var hit EsSearch
			if err := decoder.Decode(&hit); err != nil {
				if err == io.EOF {
					break
				}
				pb.Describe(fmt.Sprintf("ERROR: %s", err))
				continue
			}

			addToIndex(bi, hit, pb, &count)
		}
	} else if *FILE_FORMAT == "csv" {
		// Create a CSV reader with the buffered reader as the source
		csvReader := csv.NewReader(bufferedReader)

		// Read the header line
		header, err := csvReader.Read()
		if err != nil {
			log.Fatalf("Error reading CSV header: %s", err)
		}

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Printf("Error reading CSV record: %s", err)
				if *CSV_ERROR_FILE != "" {
					f, err := os.OpenFile(*CSV_ERROR_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Fatalf("Error opening CSV error file: %s", err)
					}
					if _, err := f.WriteString(strings.Join(record, ",") + "\n"); err != nil {
						log.Fatalf("Error writing CSV error file: %s", err)
					}
					f.Close()
				}
				errCount++
				continue
			}

			// Create a map for the current record
			var hit EsSearch = make(map[string]interface{})
			for i, val := range record {
				hit[header[i]] = val
			}

			addToIndex(bi, hit, pb, &count)
		}
	} else {
		log.Fatalf("Unknown import type: %s", *FILE_FORMAT)
	}

	// Index the remaining documents in the buffer
	if err := bi.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	fmt.Println("\nDone, imported", count, "documents to", *ES_INDEX, "error", errCount)
}

func createIndex(es *elasticsearch.Client, indexName string) {
	// Define the index mapping
	mapping := `
	{
		"mappings": {
			"date_detection": false,
			"numeric_detection": false,
			"properties": {
				"data": {
					"type": "object"
				}
			}
		}
	}`

	// Create the index with the defined mapping
	res, err := es.Indices.Create(
		indexName,
		es.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		log.Fatalf("Error creating the index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Error response from server: %s", res)
	}

	defer res.Body.Close()

	fmt.Printf("Index %s created successfully.\n", indexName)
}

func addToIndex(bi esutil.BulkIndexer, hit EsSearch, pb *progressbar.ProgressBar, count *uint64) {
	for _, field := range *IGNORE_FIELD {
		delete(hit, field)
	}
	data, err := json.Marshal(hit)
	if err != nil {
		log.Fatalf("Cannot encode record %s: %s", hit, err)
	}
	bi.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Action: "create",

			// Body is an `io.Reader` with the payload
			Body: bytes.NewReader(data),

			// OnSuccess is called for each successful operation
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				pb.Add(1)
				atomic.AddUint64(count, 1)
			},

			// OnFailure is called for each failed operation
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					pb.Describe("ERROR: " + err.Error())
				} else {
					pb.Describe(fmt.Sprintf("ERROR: %s: %s", res.Error.Type, res.Error.Reason))
				}
			},
		},
	)
}
