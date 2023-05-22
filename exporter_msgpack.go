package main

import "github.com/vmihailenco/msgpack"

type MsgPackExporter struct {
	encoder *msgpack.Encoder
}

func (e *MsgPackExporter) Encode(v interface{}) error {
	return e.encoder.Encode(v)
}

func (e *MsgPackExporter) Flush() error {
	return nil
}

func (e *MsgPackExporter) SetFields(fields []string) error {
	// This is a no-op for MsgPackExporter
	return nil
}
