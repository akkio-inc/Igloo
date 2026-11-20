package federation

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The PostgreSQL side of the proxy.
//
// DuckDB's postgres extension takes a connection from its pool for every
// transaction that touches the database, and opens one when the pool has
// none that works; it begins a remote transaction even to list tables it
// already knows, and resets the session when it returns it to the pool. If
// all of that went to the server, a server that is down or hanging would
// make listing any catalog's tables (information_schema, psql's \dt, the
// grant checks of package pgwire) fail or hang for everyone.
//
// So the proxy answers what needs no server itself. It completes DuckDB's
// sign-in with the parameters the server reported the last time Igloo
// signed in, and holds back BEGIN until DuckDB sends a statement that needs
// the server, answering a transaction with no such statement, the pool's
// health check (SELECT 1) and the reset of a session that ran nothing
// (DISCARD ALL) by itself. A held BEGIN goes to the server just before the
// first statement that does, which is when PostgreSQL takes a
// transaction's snapshot anyway. The proxy signs in to the server when that
// first statement comes. If the server cannot be reached then, the
// statement fails as one the server refused would, with an error that names
// the connection, and the session goes on: the next statement tries again.
// Ending the session would hide the error, as libpq reports the lost
// connection in its place.
//
// # Cancel requests
//
// Igloo's build of the postgres extension (extensions/postgres) sends a
// cancel request for a scan its statement no longer needs: the statement
// was canceled, it failed, or it has its rows. The proxy passes the request
// on to the server, for the session it names and no other, so the scan's
// COPY ends there instead of being read to its end.
//
// PostgreSQL fails the transaction of a statement it cancels, and a scan's
// transaction may go on after it: a statement's only scan reads its first
// task on the connection of DuckDB's transaction, where the next statement
// of a transaction block reads too. The proxy cannot tell such a
// transaction from one that ends with its statement, as DuckDB begins both
// alike. So a COPY inside a transaction runs in a savepoint of the proxy's.
// When the server has canceled the COPY, the proxy tells DuckDB that the
// transaction is still good, and rolls back to the savepoint before
// DuckDB's next statement: the transaction goes on, with the snapshot it
// had, which a transaction begun anew would not have. The savepoint costs
// one statement in front of a connection's first scan of a transaction,
// sent with it. It is released before a statement that is not a scan, as
// pg_export_snapshot refuses to run inside one, and ends with the
// transaction otherwise.
//
// A cancel request for a statement the proxy cannot shield so, which is
// any other statement inside a transaction, is dropped: the statement runs
// to its end, as every one did before. A request the proxy does pass on
// goes after a moment in which its statement may end by itself, and
// travels while DuckDB reads on (see cancelPostgres).

// maxMessage bounds a message from DuckDB, which sends statements and
// their parameters.
const maxMessage = 1 << 30

// maxPasswordMessage bounds the message that carries the token. Anyone on
// the host can connect to the loopback port and claim a body of any length,
// which would otherwise be allocated before the token is checked.
const maxPasswordMessage = 1024

// servePostgres signs DuckDB in with the token and runs its session.
func (p *proxy) servePostgres(client net.Conn, c Connection) error {
	// DuckDB has as long as the remote database to finish signing in.
	_ = client.SetDeadline(time.Now().Add(c.ConnectTimeout + 5*time.Second))
	be := pgproto3.NewBackend(client, client)
	var startup *pgproto3.StartupMessage
	for startup == nil {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			return errClientGone
		}
		switch m := msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			// The hop from DuckDB stays on the loopback interface.
			if _, err := client.Write([]byte{'N'}); err != nil {
				return errClientGone
			}
		case *pgproto3.StartupMessage:
			startup = m
		case *pgproto3.CancelRequest:
			// DuckDB waits for the connection to close, as the sign that
			// the server has the request.
			p.cancelPostgres(m.ProcessID, m.SecretKey)
			return nil
		default:
			return nil
		}
	}

	s := &pgSession{
		p: p, startup: startup.Parameters, client: client,
		out: bufio.NewWriterSize(client, 64<<10), r: newRelay(client, c.QueryTimeout),
	}
	s.r.quiet = func() bool { return s.status == 'I' }
	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := be.Flush(); err != nil {
		return errClientGone
	}
	if err := be.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return err
	}
	be.SetMaxBodyLen(maxPasswordMessage)
	msg, err := be.Receive()
	if err != nil {
		return errClientGone
	}
	if pw, ok := msg.(*pgproto3.PasswordMessage); !ok || !p.checkToken([]byte(pw.Password)) {
		return s.fatal("28P01", errors.New("wrong proxy token"))
	}

	params := p.serverParams()
	var pid uint32
	var key []byte
	if params == nil {
		// Never signed in: sign in now, to learn the parameters.
		if err := s.connect(); err != nil {
			return s.fatal("08001", err)
		}
		params, pid, key = s.params, s.pid, s.key
	} else {
		var b [8]byte
		_, _ = rand.Read(b[:])
		pid, key = binary.BigEndian.Uint32(b[:4])&^(1<<31), b[4:]
	}
	if startup.ProtocolVersion != pgproto3.ProtocolVersion30 {
		// The session with the remote server speaks 3.0.
		be.Send(&pgproto3.NegotiateProtocolVersion{NewestMinorProtocol: 0})
	}
	be.Send(&pgproto3.AuthenticationOk{})
	for k, v := range params {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	// DuckDB names the session by these in a cancel request, so the
	// session is known by them before DuckDB learns them.
	s.clientPID, s.clientKey = pid, key
	if !p.track(s.r) {
		s.r.close()
		return errors.New("connection dropped")
	}
	defer p.untrack(s.r)
	p.mu.Lock()
	if p.pg == nil {
		p.pg = map[*pgSession]bool{}
	}
	p.pg[s] = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.pg, s)
		p.mu.Unlock()
	}()
	be.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: key})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		s.r.close()
		return errClientGone
	}
	_ = client.SetDeadline(time.Time{})

	err = s.run(bufio.NewReaderSize(client, 64<<10))
	s.r.close()
	s.r.mu.Lock()
	if c := s.cancel; c != nil {
		c.stop()
	}
	s.r.mu.Unlock()
	if s.upstream != nil {
		if upErr := <-s.upDone; errors.Is(err, errClientGone) && upErr != nil {
			err = upErr
		}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		p.log.Warn("the remote database left a statement unanswered past query_timeout; gave up on it",
			"query_timeout", c.QueryTimeout)
	}
	return err
}

// pgSession is one DuckDB connection through the proxy.
type pgSession struct {
	p       *proxy
	startup map[string]string
	client  net.Conn
	r       *relay
	// clientPID and clientKey are what DuckDB names the session by in a
	// cancel request.
	clientPID uint32
	clientKey []byte

	// outMu serializes writes to DuckDB from run and fromServer. run
	// writes only while the server owes DuckDB nothing.
	outMu sync.Mutex
	out   *bufio.Writer

	// These belong to run.
	upstream net.Conn
	upOut    *bufio.Writer
	// upDone reports how relaying the server's messages ended.
	upDone chan error
	params map[string]string
	pid    uint32
	key    []byte
	// held is a BEGIN DuckDB sent that the server has not seen yet.
	held []byte
	// dirty is set once the server ran a statement since the session was
	// last reset.
	dirty bool
	// aborted is set when a statement of a transaction the server had not
	// seen failed for want of the server: as on the server, nothing more
	// runs until DuckDB ends the transaction.
	aborted bool
	// skipping is set when a message of the extended protocol failed for
	// want of the server: as the server does, the proxy then discards
	// DuckDB's messages until its next Sync.
	skipping bool

	// These are under r.mu.
	// status is the transaction status the server last reported.
	status byte
	// owed are the statements of the proxy's own that the server has yet
	// to answer, in the order they were sent. DuckDB must not see their
	// answers: it had its own to a held BEGIN, and knows of no savepoint.
	// Each was sent just before a statement of DuckDB's.
	owed []ownStatement
	// refused is set once the server refused the first of owed.
	refused bool
	// savepoint is how far the proxy's savepoint is.
	savepoint savepointState
	// canceled is set when a cancel request was passed on for the
	// statement the server is at, and cancel is that request while it is on
	// its way to the server.
	canceled bool
	cancel   *pgCancel
	// backend is the session's process on the server, while there is one.
	backend *pgBackend
}

// scanSavepoint is the savepoint a COPY inside a transaction runs in, so
// that canceling it leaves the transaction usable. PostgreSQL has had
// savepoints since 8.0, in read-only transactions and on standbys too.
const scanSavepoint = "igloo_scan"

// savepointState is how far a session's savepoint is.
type savepointState int

const (
	// noSavepoint: the transaction, if there is one, is in none.
	noSavepoint savepointState = iota
	// savepointAsked: the server has yet to answer SAVEPOINT, or the
	// ROLLBACK TO SAVEPOINT that mends a failed one.
	savepointAsked
	// savepointSet: the transaction is inside the savepoint.
	savepointSet
	// savepointFailed: the server canceled a COPY inside the savepoint and
	// refuses everything until a ROLLBACK TO SAVEPOINT. DuckDB was told the
	// transaction is still good.
	savepointFailed
)

// ownStatement is a kind of statement the proxy sends of its own accord.
type ownStatement int

const (
	// ownBegin is a held BEGIN.
	ownBegin ownStatement = iota
	// ownSavepoint leaves the transaction inside the savepoint.
	ownSavepoint
	// ownRelease leaves it outside.
	ownRelease
)

// fatal tells DuckDB the session failed and why.
func (s *pgSession) fatal(code string, err error) error {
	msg := fmt.Sprintf("connection %s: %v", s.p.name, err)
	buf, _ := (&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: Scrub(msg)}).Encode(nil)
	s.outMu.Lock()
	_, _ = s.out.Write(buf)
	_ = s.out.Flush()
	s.outMu.Unlock()
	return errors.New(msg)
}

// fail fails a statement that needs the server while the proxy has none,
// as the server fails one: with an error, after which the session goes on.
// A simple query is answered at once; after a message of the extended
// protocol, the answer waits for DuckDB's Sync. A transaction DuckDB began
// is aborted.
func (s *pgSession) fail(typ byte, code string, err error) {
	if s.held != nil {
		s.held, s.aborted = nil, true
	}
	e := &pgproto3.ErrorResponse{
		Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code,
		Message: Scrub(fmt.Sprintf("connection %s: %v", s.p.name, err)),
	}
	if typ != 'Q' && typ != 'S' {
		s.skipping = true
		s.send(e)
		return
	}
	s.reply(s.emulatedStatus(), e)
}

// emulatedStatus is the transaction status DuckDB's session has while the
// proxy has no session with the server.
func (s *pgSession) emulatedStatus() byte {
	switch {
	case s.aborted:
		return 'E'
	case s.held != nil:
		return 'T'
	}
	return 'I'
}

// reply answers DuckDB without the server.
func (s *pgSession) reply(status byte, msgs ...pgproto3.BackendMessage) bool {
	s.send(append(msgs, &pgproto3.ReadyForQuery{TxStatus: status})...)
	return true
}

// send writes messages to DuckDB.
func (s *pgSession) send(msgs ...pgproto3.BackendMessage) {
	var buf []byte
	for _, m := range msgs {
		buf, _ = m.Encode(buf)
	}
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, _ = s.out.Write(buf)
	_ = s.out.Flush()
}

// connect signs in to the server, with the connection's settings of the
// moment, and starts relaying its messages.
func (s *pgSession) connect() error {
	c := s.p.settings()
	hj, err := s.p.signInPostgres(c, s.startup)
	s.p.signedIn(err)
	if err != nil {
		return fmt.Errorf("cannot sign in to %s: %w", c.Address(), err)
	}
	params := maps.Clone(hj.ParameterStatuses)
	s.upstream, s.params, s.pid, s.key, s.dirty = hj.Conn, params, hj.PID, hj.SecretKey, false
	up := hj.Conn
	s.upOut = bufio.NewWriterSize(up, 64<<10)
	s.r.mu.Lock()
	s.status, s.owed, s.refused, s.savepoint, s.canceled = 'I', nil, false, noSavepoint, false
	s.backend = &pgBackend{
		addr: up.RemoteAddr(), tls: hj.TLSConfig, direct: hj.Config.SSLNegotiation == "direct",
		pid: hj.PID, key: hj.SecretKey,
	}
	s.r.mu.Unlock()
	s.r.setUpstream(up)
	done := make(chan error, 1)
	s.upDone = done
	go func() {
		err := s.fromServer(bufio.NewReaderSize(up, 64<<10))
		if s.r.upstreamLost(up) {
			err = nil
		}
		done <- err
	}()
	return nil
}

// dropped forgets a connection to the server that went away, or was
// retired, while the session was quiet: the next statement that needs the
// server signs in again.
func (s *pgSession) dropped() {
	if s.upstream != nil && s.r.takeDropped() {
		<-s.upDone
		s.upstream, s.upOut = nil, nil
		s.r.mu.Lock()
		s.backend = nil
		s.r.mu.Unlock()
	}
}

// Statements of DuckDB's that the proxy may answer itself.
const (
	stmtOther = iota
	stmtBegin
	stmtCommit
	stmtRollback
	stmtDiscard
	stmtPing
)

// classify names a simple query's statement, from its text as the postgres
// extension and its pool send it, and returns the command tag it ends with.
// The extension also sends BEGIN in front of other statements, in one
// query: that is not one to answer.
func classify(text []byte) (int, string) {
	q := strings.ToUpper(strings.TrimRight(strings.TrimSpace(strings.TrimSuffix(string(text), "\x00")), "; \t\r\n"))
	if strings.Contains(q, ";") {
		return stmtOther, ""
	}
	switch {
	case strings.HasPrefix(q, "BEGIN") || strings.HasPrefix(q, "START TRANSACTION"):
		return stmtBegin, "BEGIN"
	case q == "COMMIT" || q == "END" || q == "COMMIT TRANSACTION":
		return stmtCommit, "COMMIT"
	case q == "ROLLBACK" || q == "ABORT" || q == "ROLLBACK TRANSACTION":
		return stmtRollback, "ROLLBACK"
	case q == "DISCARD ALL":
		return stmtDiscard, "DISCARD ALL"
	case q == "SELECT 1":
		return stmtPing, "SELECT 1"
	}
	return stmtOther, ""
}

// run reads DuckDB's messages until the session ends.
func (s *pgSession) run(in *bufio.Reader) error {
	for {
		typ, msg, err := readPGMessage(in)
		if err != nil {
			return errClientGone
		}
		s.dropped()
		if typ == 'X' {
			if s.upstream != nil {
				_, _ = s.upOut.Write(msg)
				_ = s.upOut.Flush()
			}
			return errClientGone
		}
		if s.skipping {
			if typ == 'S' {
				s.skipping = false
				s.reply(s.emulatedStatus())
			}
			continue
		}
		kind, tag, scan := stmtOther, "", false
		if typ == 'Q' {
			kind, tag = classify(msg[5:])
			scan = kind == stmtOther && isCopy(msg[5:])
		}
		if scan || s.inSavepoint() {
			// What goes in front of this statement depends on how the
			// one before it ended, which fromServer notes after it has
			// sent DuckDB the end.
			s.r.settle()
		}
		if kind != stmtOther && s.r.idle() && s.answer(kind, tag, msg) {
			continue
		}
		if s.aborted {
			s.fail(typ, "25P02", errors.New("current transaction is aborted, commands ignored until end of transaction block"))
			continue
		}

		// A statement for the server, which goes after a cancel request
		// for the statement before it.
		s.awaitCancel()
		if s.upstream == nil {
			if err := s.connect(); err != nil {
				s.fail(typ, "08006", err)
				continue
			}
		}
		began := s.held != nil
		if began {
			if err := s.sendOwn(ownBegin, s.held); err != nil {
				return err
			}
			s.held = nil
		}
		if stmt, own := s.shield(kind, scan, began); stmt != "" {
			q, _ := (&pgproto3.Query{String: stmt}).Encode(nil)
			if err := s.sendOwn(own, q); err != nil {
				return err
			}
		}
		s.dirty = kind != stmtDiscard
		if typ == 'Q' || typ == 'S' {
			s.r.begin()
		}
		if _, err := s.upOut.Write(msg); err != nil {
			return err
		}
		if in.Buffered() == 0 {
			if err := s.upOut.Flush(); err != nil {
				return err
			}
		}
	}
}

// isCopy reports whether a simple query is a COPY, as the postgres
// extension sends a scan.
func isCopy(text []byte) bool {
	text = bytes.TrimLeft(text, " \t\r\n")
	return len(text) > 4 && bytes.EqualFold(text[:4], []byte("COPY")) && (text[4] == ' ' || text[4] == '(')
}

// sendOwn sends the server a statement of the proxy's own, whose answer
// DuckDB does not see.
func (s *pgSession) sendOwn(own ownStatement, msg []byte) error {
	s.r.mu.Lock()
	s.owed = append(s.owed, own)
	s.r.mu.Unlock()
	s.r.begin()
	_, err := s.upOut.Write(msg)
	return err
}

// inSavepoint reports whether the session has a savepoint of the proxy's.
func (s *pgSession) inSavepoint() bool {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	return s.savepoint != noSavepoint
}

// shield returns what the proxy sends the server in front of DuckDB's next
// statement to keep a savepoint around every COPY inside a transaction and
// around nothing else. scan says the statement is a COPY, and began that
// the transaction's BEGIN was sent just before it. Unless a BEGIN was, the
// server owes DuckDB no answer (see settle).
func (s *pgSession) shield(kind int, scan, began bool) (string, ownStatement) {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	switch {
	case kind == stmtRollback, kind == stmtCommit && s.savepoint != savepointFailed:
		// The savepoint ends with its transaction. In front of a COMMIT
		// after a failed savepoint goes the rollback to it, below: DuckDB
		// was told the transaction is good, and the server commits it.
		s.savepoint = noSavepoint
	case scan && s.savepoint == noSavepoint:
		if began || s.status == 'T' {
			s.savepoint = savepointAsked
			return "SAVEPOINT " + scanSavepoint, ownSavepoint
		}
	case scan && s.savepoint == savepointFailed:
		// The savepoint outlives the rollback: the scan runs in it.
		s.savepoint = savepointAsked
		return "ROLLBACK TO SAVEPOINT " + scanSavepoint, ownSavepoint
	case scan:
		// The scan before left the savepoint as it was.
	case s.savepoint == savepointFailed:
		s.savepoint = noSavepoint
		return "ROLLBACK TO SAVEPOINT " + scanSavepoint + "; RELEASE SAVEPOINT " + scanSavepoint, ownRelease
	case s.savepoint == savepointSet:
		s.savepoint = noSavepoint
		return "RELEASE SAVEPOINT " + scanSavepoint, ownRelease
	}
	return "", 0
}

// ready notes the end of an answer to a statement of DuckDB's, with the
// transaction status the server reports, and returns the status to tell
// DuckDB: that the transaction is good when all that failed in it is a
// COPY the server canceled at the proxy's request, inside the savepoint,
// which the proxy rolls back to before DuckDB's next statement (see
// shield).
func (s *pgSession) ready(status byte) byte {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	tell := status
	switch {
	case status == 'E' && s.canceled && s.savepoint == savepointSet:
		s.savepoint, tell = savepointFailed, 'T'
	case status != 'T':
		// The transaction is over, or it failed and DuckDB is told so:
		// nothing runs in it before its end.
		s.savepoint = noSavepoint
	}
	if c := s.cancel; c != nil && !c.sent {
		// The statement's end overtook the request for it.
		c.moot, s.cancel = true, nil
		c.stop()
	}
	s.status, s.canceled = status, false
	return tell
}

// answered notes the end of the server's answer to a statement of the
// proxy's own, with the transaction status the server reports.
func (s *pgSession) answered(status byte) {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	if s.owed[0] == ownSavepoint {
		// Refused, there is no savepoint for the COPY that follows, and no
		// cancel request is passed on for it.
		s.savepoint = savepointSet
		if s.refused {
			s.savepoint = noSavepoint
		}
	}
	s.owed, s.refused, s.status = s.owed[1:], false, status
}

// answer answers a transaction statement, health check or reset without
// the server, when that is the same as what the server would answer. It is
// called only while the server owes DuckDB nothing.
func (s *pgSession) answer(kind int, tag string, msg []byte) bool {
	open := false // the server has a transaction open
	if s.upstream != nil {
		s.r.mu.Lock()
		open = s.status != 'I'
		s.r.mu.Unlock()
	}
	held := s.held != nil
	switch {
	case s.aborted:
		// Only ending the transaction, which rolls it back, as on the
		// server. The server was never in it.
		if kind != stmtCommit && kind != stmtRollback {
			return false
		}
		s.aborted = false
		return s.reply('I', &pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
	case kind == stmtBegin && !held && !open:
		s.held = msg
		return s.reply('T', &pgproto3.CommandComplete{CommandTag: []byte(tag)})
	case (kind == stmtCommit || kind == stmtRollback) && !open:
		s.held = nil
		return s.reply('I', &pgproto3.CommandComplete{CommandTag: []byte(tag)})
	case kind == stmtDiscard && !held && !open && !s.dirty:
		return s.reply('I', &pgproto3.CommandComplete{CommandTag: []byte(tag)})
	case kind == stmtPing && !open:
		status := byte('I')
		if held {
			status = 'T'
		}
		return s.reply(status, &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("?column?"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1},
		}}, &pgproto3.DataRow{Values: [][]byte{[]byte("1")}}, &pgproto3.CommandComplete{CommandTag: []byte(tag)})
	}
	return false
}

// fromServer relays the server's messages to DuckDB, but for the answers
// to the proxy's own statements, and notes the end of each answer. DuckDB
// has every message the server has sent whole before fromServer waits for
// the server: a server that pauses in the middle of a row, as PostgreSQL
// does whenever a scan is slow to find its next rows, would otherwise leave
// up to 64 kB of rows here, and a statement that needs only those would
// wait for rows it has no use for.
func (s *pgSession) fromServer(in *bufio.Reader) error {
	var header [5]byte
	flush := func() error {
		s.outMu.Lock()
		defer s.outMu.Unlock()
		return s.out.Flush()
	}
	for {
		if in.Buffered() < len(header) {
			if err := flush(); err != nil {
				return err
			}
		}
		if _, err := io.ReadFull(in, header[:]); err != nil {
			return err
		}
		n := int64(binary.BigEndian.Uint32(header[1:])) - 4
		if n < 0 {
			return errors.New("malformed message from the server")
		}
		if int64(in.Buffered()) < n {
			if err := flush(); err != nil {
				return err
			}
		}
		typ := header[0]
		// The end of an answer is ReadyForQuery, with the transaction's
		// status.
		var status byte
		if typ == 'Z' {
			if n != 1 {
				return errors.New("malformed message from the server")
			}
			var err error
			if status, err = in.ReadByte(); err != nil {
				return err
			}
		}
		s.r.mu.Lock()
		own := len(s.owed) > 0
		if own && typ == 'E' {
			s.refused = true
		}
		s.r.mu.Unlock()
		if own {
			if typ == 'Z' {
				s.answered(status)
				s.r.end()
				continue
			}
			if _, err := in.Discard(int(n)); err != nil {
				return err
			}
			s.r.progress()
			continue
		}

		if typ == 'Z' {
			status = s.ready(status)
		}
		s.outMu.Lock()
		_, err := s.out.Write(header[:])
		if err == nil && typ == 'Z' {
			// The end of an answer goes out at once: DuckDB's next
			// statement may follow before the server sends anything.
			if err = s.out.WriteByte(status); err == nil {
				err = s.out.Flush()
			}
		} else if err == nil {
			_, err = io.CopyN(s.out, in, n)
		}
		s.outMu.Unlock()
		if err != nil {
			return err
		}
		if typ == 'Z' {
			s.r.end()
		} else {
			s.r.progress()
		}
	}
}

// How long a cancel request waits for the statement it is for to reach the
// server. DuckDB sends the request on a connection of its own, so it may
// overtake the statement on the way to the proxy, and the proxy may have
// statements of its own in front of the statement. DuckDB waits for the
// proxy to close the request's connection before its session goes on, so
// the statement of the moment is the one the request is for.
const (
	// cancelEarly is how long a request waits for a session that has sent
	// the server nothing: in most such sessions the statement has ended,
	// and DuckDB waits for nothing but the request.
	cancelEarly = 10 * time.Millisecond
	// cancelOwn is how long it waits for the server to answer the proxy's
	// own statements. DuckDB repeats a request each second.
	cancelOwn = time.Second
)

// cancelGrace is how long a cancel request waits for its statement to end
// by itself before it goes to the server. A statement that ends early
// leaves a scan in flight on each of its connections, 18 on a machine of 18
// cores, and most of those are tasks of a table, over within this once
// DuckDB reads them out. A request costs the server a connection and a
// process, and that many connections at once, statement after statement,
// lost a packet now and then on a forwarded port, which held a session up
// for a second.
const cancelGrace = 50 * time.Millisecond

// pgCancel is a cancel request on its way to the server.
type pgCancel struct {
	// stop gives up on reaching the server.
	stop context.CancelFunc
	// done is closed once the request is over: the server has acted on it,
	// or will never see it.
	done chan struct{}
	// These are under the session's r.mu: sent is set before the request
	// is written, and moot when its statement ended before that.
	sent, moot bool
}

// cancelPostgres passes on a cancel request of DuckDB's, which names one
// of its sessions, to the server that session is signed in to, for the
// statement the session is at. The request travels while DuckDB goes on
// reading the statement's answer: DuckDB is not kept waiting for a
// connection to the server, which takes a second and more when a packet is
// lost. A request the statement's end overtakes is given up, and one
// already written is waited for before the session's next statement goes
// to the server (see awaitCancel), so no request reaches the server for a
// later statement than the one it was sent for.
func (p *proxy) cancelPostgres(pid uint32, key []byte) {
	var s *pgSession
	p.mu.Lock()
	for candidate := range p.pg {
		if candidate.clientPID == pid && subtle.ConstantTimeCompare(candidate.clientKey, key) == 1 {
			s = candidate
		}
	}
	p.mu.Unlock()
	if s == nil {
		return
	}
	ctx, stop := context.WithTimeout(context.Background(), p.settings().ConnectTimeout)
	c := &pgCancel{stop: stop, done: make(chan struct{})}
	backend := s.cancelable(c)
	if backend == nil {
		stop()
		return
	}
	go func() {
		select {
		case <-time.After(cancelGrace):
		case <-ctx.Done():
		}
		err := backend.cancel(ctx, func() bool {
			s.r.mu.Lock()
			defer s.r.mu.Unlock()
			c.sent = !c.moot
			return c.sent
		})
		if err != nil && ctx.Err() == nil {
			p.log.Debug("a cancel request did not reach the remote database", "err", err)
		}
		stop()
		s.r.mu.Lock()
		if s.cancel == c {
			s.cancel = nil
		}
		s.r.mu.Unlock()
		close(c.done)
	}()
}

// cancelable returns the session's process on the server if a cancel
// request may go to it now, and notes c as the request on its way. It
// waits for the server to be at a statement of DuckDB's: one outside a
// transaction, or a COPY inside the proxy's savepoint, which a cancel
// leaves the transaction usable after. For any other statement, and while
// another request is on its way, it returns nil.
func (s *pgSession) cancelable(c *pgCancel) *pgBackend {
	started := time.Now()
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	for {
		var wait time.Duration
		switch {
		case s.r.closed || s.cancel != nil:
			return nil
		case s.r.pending == 0:
			wait = cancelEarly
		case len(s.owed) > 0:
			wait = cancelOwn
		case s.backend != nil && (s.status == 'I' || s.savepoint == savepointSet):
			s.canceled, s.cancel = true, c
			return s.backend
		default:
			return nil
		}
		if time.Since(started) >= wait {
			return nil
		}
		s.r.mu.Unlock()
		time.Sleep(time.Millisecond)
		s.r.mu.Lock()
	}
}

// awaitCancel waits for a cancel request on its way to the server to be
// over, before the session sends the server anything more.
func (s *pgSession) awaitCancel() {
	s.r.mu.Lock()
	c := s.cancel
	s.r.mu.Unlock()
	if c != nil {
		<-c.done
	}
}

// pgBackend is a session's process on the remote server, as a cancel
// request names it, and how the session reached the server.
type pgBackend struct {
	addr net.Addr
	// tls is what the session's connection is encrypted with, or nil, and
	// direct says it began with TLS, without PostgreSQL's request for it.
	tls    *tls.Config
	direct bool
	pid    uint32
	key    []byte
}

// cancel asks the server to cancel the statement the process is at, on a
// connection of its own encrypted as the session's is, and returns once
// the server has closed it: the server has acted on the request then. It
// writes the request only if send, called once the connection is ready,
// says so.
func (b *pgBackend) cancel(ctx context.Context, send func() bool) error {
	dialed, err := (&net.Dialer{}).DialContext(ctx, b.addr.Network(), b.addr.String())
	if err != nil {
		return err
	}
	defer dialed.Close() //nolint:errcheck
	// Giving up closes the connection, which ends whatever waits on it.
	defer context.AfterFunc(ctx, func() { _ = dialed.Close() })()
	conn := dialed
	if b.tls != nil {
		if !b.direct {
			req, _ := (&pgproto3.SSLRequest{}).Encode(nil)
			if _, err := conn.Write(req); err != nil {
				return err
			}
			var answer [1]byte
			if _, err := io.ReadFull(conn, answer[:]); err != nil {
				return err
			}
			if answer[0] != 'S' {
				return errors.New("the server refused TLS")
			}
		}
		encrypted := tls.Client(conn, b.tls)
		if err := encrypted.HandshakeContext(ctx); err != nil {
			return err
		}
		conn = encrypted
	}
	req, err := (&pgproto3.CancelRequest{ProcessID: b.pid, SecretKey: b.key}).Encode(nil)
	if err != nil {
		return err
	}
	if !send() {
		return nil
	}
	if _, err := conn.Write(req); err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, conn)
	return nil
}

// readPGMessage reads one message: its type and all of it, header too.
func readPGMessage(in *bufio.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(in, header[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(header[1:])) - 4
	if n < 0 || n > maxMessage {
		return 0, nil, errors.New("malformed message")
	}
	msg := make([]byte, 5+n)
	copy(msg, header[:])
	if _, err := io.ReadFull(in, msg[5:]); err != nil {
		return 0, nil, err
	}
	return header[0], msg, nil
}

// serverParams returns the parameters the remote server reported when
// Igloo last signed in, or nil.
func (p *proxy) serverParams() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.params
}

// signInPostgres connects to the remote server as the connection's user,
// and returns the connection, with what the server reported and how it
// names the session. The session is read-only and its statements time out
// after query_timeout; the rest of the startup parameters are DuckDB's.
func (p *proxy) signInPostgres(c Connection, clientParams map[string]string) (*pgconn.HijackedConn, error) {
	password, err := p.password(c)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := tlsConfig(&c)
	if err != nil {
		return nil, err
	}
	// ParseConfig is the only way to make a pgconn.Config. Every setting
	// it takes from the environment is replaced below.
	cfg, err := pgconn.ParseConfig("sslmode=disable")
	if err != nil {
		return nil, err
	}
	cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password = c.Host, uint16(c.Port), c.Database, c.User, password
	cfg.TLSConfig, cfg.Fallbacks = tlsCfg, nil
	if c.TLS == TLSPrefer {
		cfg.Fallbacks = []*pgconn.FallbackConfig{{Host: c.Host, Port: uint16(c.Port)}}
	}
	cfg.ConnectTimeout = c.ConnectTimeout
	cfg.DialFunc = (&net.Dialer{KeepAlive: keepAlive}).DialContext
	cfg.ValidateConnect, cfg.AfterConnect, cfg.OnNotice, cfg.OnNotification = nil, nil, nil, nil
	cfg.KerberosSrvName, cfg.KerberosSpn = "", ""
	cfg.RuntimeParams = map[string]string{"application_name": "igloo"}
	for k, v := range clientParams {
		switch k {
		case "user", "database", "replication", "options":
		default:
			cfg.RuntimeParams[k] = v
		}
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	if c.QueryTimeout > 0 {
		cfg.RuntimeParams["statement_timeout"] = strconv.FormatInt(c.QueryTimeout.Milliseconds(), 10)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.ConnectTimeout)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, unwrapConnectError(err)
	}
	hj, err := conn.Hijack()
	if err != nil {
		_ = conn.Close(context.Background())
		return nil, err
	}
	p.mu.Lock()
	p.params = maps.Clone(hj.ParameterStatuses)
	p.mu.Unlock()
	return hj, nil
}

// unwrapConnectError keeps pgconn's error to its cause, without the
// connection details and fallback attempts it adds.
func unwrapConnectError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return fmt.Errorf("%s (SQLSTATE %s)", pgErr.Message, pgErr.Code)
	}
	for {
		switch inner := err.(type) {
		case interface{ Unwrap() []error }:
			errs := inner.Unwrap()
			if len(errs) == 0 {
				return err
			}
			err = errs[len(errs)-1]
		case interface{ Unwrap() error }:
			next := inner.Unwrap()
			if next == nil {
				return err
			}
			err = next
		default:
			return err
		}
	}
}
