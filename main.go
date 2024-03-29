package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/elastic/go-elasticsearch/v8"
)

var (
	// Sub Command
	EXPORT_COMMAND = kingpin.Command("export", "Export Elasticsearch index to a file")
	IMPORT_COMMAND = kingpin.Command("import", "Import Elasticsearch index from a file")
)

var (
	ES_URL = kingpin.Flag("url", "Elasticsearch URL").
		Envar("ES_URL").
		Default("http://localhost:9200").
		String()
	ES_USER = kingpin.Flag("user", "Elasticsearch user").
		Envar("ES_USER").
		Default("elastic").
		String()
	ES_PASS = kingpin.Flag("pass", "Elasticsearch password").
		Envar("ES_PASS").
		String()
	ES_INDEX = kingpin.Flag("index", "Elasticsearch index").
			Envar("ES_INDEX").
			Required().
			String()

	FILE_FORMAT = kingpin.Flag("file-format", "csv (default) for flat structure/more space efficient, msgpack for inconsistent/complex document.").
			Envar("FILE_FORMAT").
			Default("csv").
			String()

	ES_BATCH_SIZE = EXPORT_COMMAND.Flag("batch-size", "Elasticsearch export/dump batch size").
			Envar("ES_BATCH_SIZE").
			Default("1000").
			Int()

	OUTPUT = EXPORT_COMMAND.Flag("output", "Output file, used for export/dump").
		Envar("OUTPUT_FILE").
		Default("esdump").
		String()
	INPUT = IMPORT_COMMAND.Flag("input", "Input file, used for import").
		Envar("INPUT_FILE").
		String()

	CREATE_INDEX = IMPORT_COMMAND.Flag("create-index", "Create index before importing data").
			Envar("CREATE_INDEX").
			Bool()

	IGNORE_FIELD = kingpin.Flag("ignore-field", "Ignore field when importing data, separated by comma").
			Envar("IGNORE_FIELD").
			Strings()

	CSV_ERROR_FILE = kingpin.Flag("csv-error-file", "Used to store invalid csv line while importing data").
			Envar("CSV_ERROR_FILE").
			Default("").
			String()

	DEBUG = kingpin.Flag("debug", "Enable debug mode").
		Envar("DEBUG").
		Default("false").
		Bool()
)

func main() {
	cmd := kingpin.Parse()

	cfg := elasticsearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{*ES_URL},
		Username:  *ES_USER,
		Password:  *ES_PASS,
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	switch cmd {
	case EXPORT_COMMAND.FullCommand():
		exportData(es)
	case IMPORT_COMMAND.FullCommand():
		importData(es)
	default:
		fmt.Println("expected 'export' or 'import' subcommands")
		os.Exit(1)
	}
}
