package main

import (
	"encoding/csv"
	"fmt"
)

type CSVExporter struct {
	writer *csv.Writer
	fields []string
}

func (e *CSVExporter) Encode(v interface{}) error {
	if hit, ok := v.(map[string]interface{}); ok {
		record := make([]string, len(e.fields))
		for i, field := range e.fields {
			if value, ok := hit[field]; ok {
				record[i] = fmt.Sprint(value)
			} else {
				record[i] = ""
			}
		}
		return e.writer.Write(record)
	} else {
		return fmt.Errorf("invalid type for v, expected map[string]interface{}, got %T", v)
	}
}

func (e *CSVExporter) SetFields(fields []string) error {
	e.fields = fields
	// Write header
	return e.writer.Write(fields)
}

func (e *CSVExporter) Flush() error {
	e.writer.Flush()
	if err := e.writer.Error(); err != nil {
		return err
	}
	return nil
}
