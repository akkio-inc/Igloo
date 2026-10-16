package federation

import (
	"bufio"
	"context"
	"crypto/rand"
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
// first statement comes; if the server cannot be reached then, the
// statement fails and the session ends.

// maxMessage bounds a message from DuckDB, which sends statements and
// their parameters.
const maxMessage = 1 << 30

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
		default:
			// Cancel requests: the extension does not send them.
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
	be.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: key})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		s.r.close()
		return errClientGone
	}
	_ = client.SetDeadline(time.Time{})

	if !p.track(s.r) {
		s.r.close()
		return errors.New("connection dropped")
	}
	defer p.untrack(s.r)
	err = s.run(bufio.NewReaderSize(client, 64<<10))
	s.r.close()
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

	// These are under r.mu.
	// status is the transaction status the server last reported.
	status byte
	// swallow counts answers from the server that DuckDB must not see:
	// those to held BEGINs, which DuckDB had its answer to.
	swallow int
}

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

// reply answers DuckDB without the server.
func (s *pgSession) reply(status byte, msgs ...pgproto3.BackendMessage) bool {
	var buf []byte
	for _, m := range append(msgs, &pgproto3.ReadyForQuery{TxStatus: status}) {
		buf, _ = m.Encode(buf)
	}
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, _ = s.out.Write(buf)
	_ = s.out.Flush()
	return true
}

// connect signs in to the server, with the connection's settings of the
// moment, and starts relaying its messages.
func (s *pgSession) connect() error {
	c := s.p.settings()
	up, params, pid, key, err := s.p.signInPostgres(c, s.startup)
	s.p.signedIn(err)
	if err != nil {
		return fmt.Errorf("cannot sign in to %s: %w", c.Address(), err)
	}
	s.upstream, s.params, s.pid, s.key, s.dirty = up, params, pid, key, false
	s.upOut = bufio.NewWriterSize(up, 64<<10)
	s.r.mu.Lock()
	s.status = 'I'
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
		kind, tag := stmtOther, ""
		if typ == 'Q' {
			kind, tag = classify(msg[5:])
		}
		if kind != stmtOther && s.r.idle() && s.answer(kind, tag, msg) {
			continue
		}

		// A statement for the server.
		if s.upstream == nil {
			if err := s.connect(); err != nil {
				return s.fatal("08006", err)
			}
		}
		if s.held != nil {
			s.r.mu.Lock()
			s.swallow++
			s.r.mu.Unlock()
			s.r.begin()
			if _, err := s.upOut.Write(s.held); err != nil {
				return err
			}
			s.held = nil
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
// to held BEGINs, and notes the end of each answer.
func (s *pgSession) fromServer(in *bufio.Reader) error {
	var header [5]byte
	for {
		if _, err := io.ReadFull(in, header[:]); err != nil {
			return err
		}
		n := int64(binary.BigEndian.Uint32(header[1:])) - 4
		if n < 0 {
			return errors.New("malformed message from the server")
		}
		typ := header[0]
		s.r.mu.Lock()
		swallowing := s.swallow > 0
		s.r.mu.Unlock()
		if swallowing {
			if _, err := in.Discard(int(n)); err != nil {
				return err
			}
			if typ == 'Z' {
				s.r.mu.Lock()
				s.swallow--
				s.r.mu.Unlock()
				s.r.end()
			} else {
				s.r.progress()
			}
			continue
		}

		var status byte
		s.outMu.Lock()
		_, err := s.out.Write(header[:])
		if err == nil && typ == 'Z' && n == 1 {
			if status, err = in.ReadByte(); err == nil {
				err = s.out.WriteByte(status)
			}
		} else if err == nil {
			_, err = io.CopyN(s.out, in, n)
		}
		if err == nil && in.Buffered() == 0 {
			err = s.out.Flush()
		}
		s.outMu.Unlock()
		if err != nil {
			return err
		}
		if typ == 'Z' {
			s.r.mu.Lock()
			s.status = status
			s.r.mu.Unlock()
			s.r.end()
		} else {
			s.r.progress()
		}
	}
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

// signInPostgres connects to the remote server as the connection's user.
// The session is read-only and its statements time out after
// query_timeout; the rest of the startup parameters are DuckDB's.
func (p *proxy) signInPostgres(c Connection, clientParams map[string]string) (net.Conn, map[string]string, uint32, []byte, error) {
	password, err := p.password(c)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	tlsCfg, err := tlsConfig(&c)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	// ParseConfig is the only way to make a pgconn.Config. Every setting
	// it takes from the environment is replaced below.
	cfg, err := pgconn.ParseConfig("sslmode=disable")
	if err != nil {
		return nil, nil, 0, nil, err
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
		return nil, nil, 0, nil, unwrapConnectError(err)
	}
	hj, err := conn.Hijack()
	if err != nil {
		_ = conn.Close(context.Background())
		return nil, nil, 0, nil, err
	}
	params := maps.Clone(hj.ParameterStatuses)
	p.mu.Lock()
	p.params = params
	p.mu.Unlock()
	return hj.Conn, params, hj.PID, hj.SecretKey, nil
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
