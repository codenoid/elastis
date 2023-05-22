# Elastis

Export, Import and Copy (between two host) Elasticsearch data with ease.

## Help

```ps1
$ elastis --help-long
usage: elastis.exe --pass=PASS --index=INDEX [<flags>] <command> [<args> ...]

Flags:
  --[no-]help                    Show context-sensitive help (also try
                                 --help-long and --help-man).
  --url="http://localhost:9200"  Elasticsearch URL ($ES_URL)
  --user="elastic"               Elasticsearch user ($ES_USER)
  --pass=PASS                    Elasticsearch password ($ES_PASS)
  --index=INDEX                  Elasticsearch index ($ES_INDEX)
  --file-format="csv"            csv (default) for flat structure/more space
                                 efficient, msgpack for inconsistent/complex
                                 document. ($FILE_FORMAT)

Commands:
help [<command>...]
    Show help.


export [<flags>]
    Export Elasticsearch index to a file

    --batch-size=1000  Elasticsearch export/dump batch size ($ES_BATCH_SIZE)
    --output="esdump"  Output file, used for export/dump ($OUTPUT_FILE)

import [<flags>]
    Import Elasticsearch index from a file

    --input=INPUT        Input file, used for import ($INPUT_FILE)
    --[no-]create-index  Create index before importing data ($CREATE_INDEX)
```
