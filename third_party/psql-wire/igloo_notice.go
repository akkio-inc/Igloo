package wire

import "github.com/jeroenrinzema/psql-wire/pkg/types"

// Noticer is implemented by the DataWriter of a statement run for a client
// connection. Notice sends the client a NoticeResponse with severity
// WARNING, before the statement completes, as PostgreSQL sends warnings;
// psql and libpq print it, and drivers hand it to a notice handler.
type Noticer interface {
	Notice(code, message string) error
}

func (writer *dataWriter) Notice(code, message string) error {
	if writer.closed {
		return ErrClosedWriter
	}
	writer.client.Start(types.ServerNoticeResponse)
	for _, f := range []struct {
		field errFieldType
		value string
	}{{errFieldSeverity, "WARNING"}, {'V', "WARNING"}, {errFieldSQLState, code}, {errFieldMsgPrimary, message}} {
		writer.client.AddByte(byte(f.field))
		writer.client.AddString(f.value)
		writer.client.AddNullTerminate()
	}
	writer.client.AddNullTerminate()
	return writer.client.End()
}
