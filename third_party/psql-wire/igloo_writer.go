package wire

import (
	"errors"
	"fmt"

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

// CopyFramer is a CopyOuter that also takes CopyData messages whole.
type CopyFramer interface {
	// CopyFrames sends data, whole CopyData messages, each with its type
	// byte and length, as they are: the rows of a copy its statement
	// encoded itself, many to a write. They follow what CopyData was given.
	CopyFrames(data []byte) error
}

func (writer *dataWriter) CopyFrames(data []byte) error {
	if err := writer.client.FlushBatch(); err != nil {
		return err
	}
	_, err := writer.client.Write(data)
	return err
}

// EncodedRowWriter is a DataWriter that takes rows a statement encoded
// itself. Row and Rows take each value as a Go value and find, for each, how
// its type is encoded: most of the time a large result takes. A statement
// that reads its rows where its engine holds them writes each value's bytes
// once, a column at a time.
type EncodedRowWriter interface {
	// RowFormats returns the formats the client asked for the columns'
	// values in: none for text, one for every column, or one a column.
	RowFormats() []FormatCode
	// RowLimit returns how many rows the client takes before it reads on,
	// the rest of a portal's row limit, or 0 for no limit. EncodedRows
	// takes no more than that at a time.
	RowLimit() int
	// EncodedRows sends data, n whole DataRow messages, each with its type
	// byte and length, in the columns' formats.
	EncodedRows(n int, data []byte) error
}

func (writer *dataWriter) RowFormats() []FormatCode {
	return writer.formats
}

func (writer *dataWriter) RowLimit() int {
	if writer.batchable || writer.remaining == nil {
		return 0
	}
	return int(*writer.remaining)
}

func (writer *dataWriter) EncodedRows(n int, data []byte) error {
	if writer.closed {
		return ErrClosedWriter
	}
	if limit := writer.RowLimit(); limit > 0 && n > limit {
		return fmt.Errorf("%d encoded rows for a client that takes %d", n, limit)
	}
	if err := writer.ctx.Err(); err != nil {
		return err
	}
	if _, err := writer.client.Write(data); err != nil {
		return err
	}
	writer.written += uint32(n)
	// As after a row, or a batch of them: the client's Execute may have its
	// rows now, or its portal may be closed.
	if !writer.yield(n) {
		return ErrSuspendedHandlerClosed
	}
	return nil
}

// Drainer is a DataWriter that says whether the statement runs to its end
// in one go.
type Drainer interface {
	Drains() bool
}

// Drains reports whether the client reads every row of the statement in
// this Execute, without a row limit, so that the statement runs to its end
// before the session handles another message. A portal with a row limit
// suspends the statement after that many rows, and the client may run
// other statements before it reads more.
func (writer *dataWriter) Drains() bool {
	return writer.batchable
}

// CopyIner is a DataWriter that can receive the data of COPY ... FROM STDIN.
type CopyIner interface {
	CopyInColumns(format FormatCode, columns int) (*CopyReader, error)
}

// CopyInColumns starts COPY ... FROM STDIN: it sends a CopyInResponse for
// columns in format, whatever columns the statement was described with (a
// COPY returns no rows), and returns the reader of what the client sends.
func (writer *dataWriter) CopyInColumns(format FormatCode, columns int) (*CopyReader, error) {
	if writer.closed {
		return nil, ErrClosedWriter
	}
	writer.client.Start(types.ServerCopyInResponse)
	writer.client.AddByte(byte(format))
	writer.client.AddInt16(int16(columns))
	for range columns {
		writer.client.AddInt16(int16(format))
	}
	if err := writer.client.End(); err != nil {
		return nil, err
	}
	return NewCopyReader(writer.session, writer.reader, writer.client, nil), nil
}
