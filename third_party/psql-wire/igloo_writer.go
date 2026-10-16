package wire

import (
	"errors"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// Igloo patch: lets a statement describe its rows when it runs rather than
// when it is parsed, and send the messages of COPY ... TO STDOUT.

// ColumnDefiner is a DataWriter that can describe a statement's rows once
// the statement runs.
type ColumnDefiner interface {
	DefineColumns(columns Columns) error
}

// DefineColumns sends the description of the rows a statement is about to
// write, for a statement parsed without columns: Igloo runs the statements
// of a simple query one at a time, and prepares each only once the one
// before has run. It must come before the first row.
func DefineColumns(w DataWriter, columns Columns) error {
	if d, ok := w.(ColumnDefiner); ok {
		return d.DefineColumns(columns)
	}
	return errors.New("this writer cannot describe columns while running")
}

func (writer *dataWriter) DefineColumns(columns Columns) error {
	if writer.written > 0 || len(writer.columns) > 0 {
		return errors.New("the statement's columns are already described")
	}
	writer.columns = columns
	return columns.Define(writer.ctx, writer.client, writer.formats)
}

// CopyOuter is a DataWriter that can send the data of COPY ... TO STDOUT.
type CopyOuter interface {
	CopyOut(format FormatCode, columns int) error
	CopyData(data []byte) error
	CopyDone() error
	CopyAbort() error
}

// CopyOut starts COPY ... TO STDOUT: it sends a CopyOutResponse for
// columns in format. Send the data with CopyData and end with CopyDone,
// then complete the statement with its "COPY n" tag; or, when it fails,
// call CopyAbort and return the error, which ends the copy for the client.
func (writer *dataWriter) CopyOut(format FormatCode, columns int) error {
	if writer.closed {
		return ErrClosedWriter
	}
	writer.client.Start(types.ServerMessage('H'))
	writer.client.AddByte(byte(format))
	writer.client.AddInt16(int16(columns))
	for range columns {
		writer.client.AddInt16(int16(format))
	}
	if err := writer.client.End(); err != nil {
		return err
	}
	// CopyData messages are small; send them in large writes.
	writer.client.StartBatch(32 << 10)
	return nil
}

func (writer *dataWriter) CopyData(data []byte) error {
	writer.client.Start(types.ServerMessage('d'))
	writer.client.AddBytes(data)
	return writer.client.End()
}

func (writer *dataWriter) CopyDone() error {
	writer.client.Start(types.ServerMessage('c'))
	return errors.Join(writer.client.End(), writer.client.EndBatch())
}

func (writer *dataWriter) CopyAbort() error {
	return writer.client.EndBatch()
}
