package federation

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // MySQL's password protocols are built on SHA-1
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The MySQL side of the proxy. It greets DuckDB's MySQL client as a server
// would, checks the token, signs in to the remote server itself and then
// relays packets. MySQL's protocol has no message that ends every answer,
// so the proxy follows the commands DuckDB's client sends and the shape of
// each answer (mysqlTracker), which also lets it refuse a server's request
// for a local file.

// Capability flags (CLIENT_* in MySQL's protocol documentation).
const (
	capLongPassword        uint32 = 1 << 0 // MariaDB servers clear it: CLIENT_MYSQL
	capConnectWithDB       uint32 = 1 << 3
	capCompress            uint32 = 1 << 5
	capLocalFiles          uint32 = 1 << 7
	capProtocol41          uint32 = 1 << 9
	capSSL                 uint32 = 1 << 11
	capTransactions        uint32 = 1 << 13
	capSecureConnection    uint32 = 1 << 15
	capMultiResults        uint32 = 1 << 17
	capPluginAuth          uint32 = 1 << 19
	capConnectAttrs        uint32 = 1 << 20
	capPluginAuthLenenc    uint32 = 1 << 21
	capSessionTrack        uint32 = 1 << 23
	capDeprecateEOF        uint32 = 1 << 24
	capOptionalMetadata    uint32 = 1 << 25
	capZstdCompression     uint32 = 1 << 26
	capQueryAttributes     uint32 = 1 << 27
	capMultiFactorAuth     uint32 = 1 << 28
	capCapabilityExtension uint32 = 1 << 29
	capSSLVerifyServerCert uint32 = 1 << 30
	capRememberOptions     uint32 = 1 << 31
)

// strippedCaps are never offered to DuckDB's client: TLS and compression on
// the loopback hop, local files, and protocol variants that change the
// shape of answers, which the tracker would have to follow.
const strippedCaps = capSSL | capCompress | capZstdCompression | capLocalFiles | capOptionalMetadata |
	capQueryAttributes | capMultiFactorAuth | capCapabilityExtension | capSSLVerifyServerCert |
	capRememberOptions | capConnectAttrs

// Status flags.
const (
	statusInTrans        uint16 = 0x0001
	statusMoreResults    uint16 = 0x0008
	statusCursorExists   uint16 = 0x0040
	statusStateChanged   uint16 = 0x4000
	maxPacketPayload            = 0xFFFFFF
	nativePasswordPlugin        = "mysql_native_password"
)

// Commands.
const (
	comQuit             byte = 0x01
	comInitDB           byte = 0x02
	comQuery            byte = 0x03
	comFieldList        byte = 0x04
	comStatistics       byte = 0x09
	comPing             byte = 0x0e
	comStmtPrepare      byte = 0x16
	comStmtExecute      byte = 0x17
	comStmtSendLongData byte = 0x18
	comStmtClose        byte = 0x19
	comStmtReset        byte = 0x1a
	comSetOption        byte = 0x1b
	comStmtFetch        byte = 0x1c
	comResetConnection  byte = 0x1f
)

// Like the PostgreSQL side (see pgproxy.go), the proxy completes DuckDB's
// sign-in by itself once it has met the server, holds back START
// TRANSACTION, and answers what needs no server (an empty transaction, a
// ping, the reset of a session that ran nothing) itself, so that listing
// tables never waits on the server. It signs in to the server at the first
// command that needs it. Resetting a session that ran statements goes to
// the server, which then forgets that the session is read-only, so the
// proxy makes it so again before the next command.

// serveMySQL signs DuckDB's MySQL client in with the token and runs its
// session.
func (p *proxy) serveMySQL(client net.Conn, c Connection) error {
	_ = client.SetDeadline(time.Now().Add(c.ConnectTimeout + 5*time.Second))
	cl := &packetConn{r: bufio.NewReaderSize(client, 64<<10), w: client}
	s := &mysqlSession{p: p, client: client, out: bufio.NewWriterSize(client, 64<<10), r: newRelay(client, c.QueryTimeout)}
	s.r.quiet = func() bool { return s.t.lastStatus()&statusInTrans == 0 }

	greeting, status := p.mysqlCache()
	var up *packetConn
	if greeting == nil {
		// Never met the server: its greeting comes first.
		var err error
		if up, greeting, err = p.dialMySQL(c); err != nil {
			p.signedIn(err)
			_ = cl.writePacket(0, errPacket(2003, "08001", fmt.Sprintf("connection %s: cannot reach %s: %v", p.name, c.Address(), err)))
			return err
		}
		defer func() {
			if s.up == nil {
				_ = up.conn.Close()
			}
		}()
	}

	// Greet DuckDB's client as the remote server would, but with a scramble
	// of the proxy's own and without the capabilities it strips.
	scramble := newScramble()
	caps := greeting.caps&^strippedCaps | capProtocol41 | capSecureConnection | capPluginAuth
	if err := cl.writePacket(0, buildGreeting(greeting, scramble, caps)); err != nil {
		return errClientGone
	}
	resp, seq, err := cl.readPacket()
	if err != nil {
		return errClientGone
	}
	hs, err := parseHandshakeResponse(resp)
	if err != nil {
		_ = cl.writePacket(seq+1, errPacket(1043, "08S01", err.Error()))
		return err
	}
	if hs.plugin != nativePasswordPlugin {
		seq++
		if err := cl.writePacket(seq, authSwitch(nativePasswordPlugin, scramble)); err != nil {
			return errClientGone
		}
		if hs.auth, seq, err = cl.readPacket(); err != nil {
			return errClientGone
		}
	}
	if subtle.ConstantTimeCompare(hs.auth, nativePassword(p.token, scramble)) != 1 {
		_ = cl.writePacket(seq+1, errPacket(1045, "28000", "connection "+p.name+": wrong proxy token"))
		return errors.New("wrong proxy token")
	}
	s.hs, s.flags = hs, hs.caps&caps
	s.t = &mysqlTracker{deprecateEOF: s.flags&capDeprecateEOF != 0}

	if up != nil {
		if status, err = p.signInMySQL(up, greeting, c, s.flags, hs); err != nil {
			p.signedIn(err)
			_ = cl.writePacket(seq+1, errPacket(1045, "28000", fmt.Sprintf("connection %s: cannot sign in to %s: %v", p.name, c.Address(), err)))
			return err
		}
		p.signedIn(nil)
		s.started(up)
	}
	s.t.status = status
	if err := cl.writePacket(seq+1, okPacket(status, s.flags)); err != nil {
		return errClientGone
	}
	_ = client.SetDeadline(time.Time{})

	if !p.track(s.r) {
		s.r.close()
		return errors.New("connection dropped")
	}
	defer p.untrack(s.r)
	err = s.run(cl.r)
	s.r.close()
	if s.up != nil {
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

// mysqlCache returns the greeting and status the server sent the last time
// Igloo signed in, or nil.
func (p *proxy) mysqlCache() (*mysqlGreeting, uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.greeting, p.status
}

// mysqlSession is one DuckDB connection through the proxy.
type mysqlSession struct {
	p      *proxy
	client net.Conn
	r      *relay
	t      *mysqlTracker
	hs     *handshakeResponse
	flags  uint32

	// outMu serializes writes to DuckDB; run writes only while the server
	// owes DuckDB nothing.
	outMu sync.Mutex
	out   *bufio.Writer

	// These belong to run.
	up    *packetConn
	upOut *bufio.Writer
	// upDone reports how relaying the server's packets ended.
	upDone chan error
	// held is a START TRANSACTION the server has not seen yet.
	held []byte
	// autocommit is a SET autocommit=1 the server has not seen yet.
	autocommit bool
	// setUp says the server's session needs making read-only again, after
	// a reset.
	setUp bool
	// dirty is set once the server ran a command since the session was
	// last reset.
	dirty bool
}

// started relays the server's packets once signed in.
func (s *mysqlSession) started(up *packetConn) {
	s.up, s.dirty, s.setUp = up, false, false
	s.upOut = bufio.NewWriterSize(up.conn, 64<<10)
	s.r.setUpstream(up.conn)
	done := make(chan error, 1)
	s.upDone = done
	go func() {
		err := s.fromServer(up.r)
		if s.r.upstreamLost(up.conn) {
			err = nil
		}
		done <- err
	}()
}

// connect signs in to the server, with the connection's settings of the
// moment.
func (s *mysqlSession) connect() error {
	c := s.p.settings()
	up, g, err := s.p.dialMySQL(c)
	if err == nil {
		var status uint16
		if status, err = s.p.signInMySQL(up, g, c, s.flags, s.hs); err != nil {
			_ = up.conn.Close()
		} else {
			s.t.mu.Lock()
			s.t.status = status
			s.t.mu.Unlock()
		}
	}
	s.p.signedIn(err)
	if err != nil {
		return fmt.Errorf("cannot sign in to %s: %w", c.Address(), err)
	}
	s.started(up)
	return nil
}

// dropped forgets a connection to the server that went away, or was
// retired, while the session was quiet: the next command that needs the
// server signs in again.
func (s *mysqlSession) dropped() {
	if s.up != nil && s.r.takeDropped() {
		<-s.upDone
		s.up, s.upOut = nil, nil
	}
}

// Statements of DuckDB's MySQL client that the proxy may answer itself.
const (
	myOther = iota
	myBegin
	myEnd
	myAutocommit
)

// classifyMySQL names a COM_QUERY's statement, from its text as the mysql
// extension sends it.
func classifyMySQL(text []byte) int {
	q := strings.ToUpper(strings.Join(strings.Fields(strings.TrimRight(string(text), "; \t\r\n")), " "))
	if strings.Contains(q, ";") {
		return myOther
	}
	switch {
	case q == "BEGIN" || strings.HasPrefix(q, "START TRANSACTION"):
		return myBegin
	case q == "COMMIT" || q == "ROLLBACK":
		return myEnd
	case strings.ReplaceAll(q, " ", "") == "SETAUTOCOMMIT=1":
		return myAutocommit
	}
	return myOther
}

// run reads DuckDB's commands until the session ends.
func (s *mysqlSession) run(in *bufio.Reader) error {
	for {
		n, header, err := readHeader(in)
		if err != nil {
			return errClientGone
		}
		if n >= maxPacketPayload {
			// A command too long for one packet is never one to answer.
			return errors.New("refused a command longer than 16 MiB")
		}
		pkt := make([]byte, 4+n)
		copy(pkt, header[:])
		if _, err := io.ReadFull(in, pkt[4:]); err != nil {
			return errClientGone
		}
		if n == 0 {
			return errors.New("empty command")
		}
		s.dropped()
		cmd := pkt[4]
		if cmd == comQuit {
			if s.up != nil {
				_, _ = s.upOut.Write(pkt)
				_ = s.upOut.Flush()
			}
			return errClientGone
		}
		if s.r.idle() && s.answer(cmd, pkt) {
			continue
		}

		// A command for the server.
		if s.up == nil {
			if err := s.connect(); err != nil {
				_ = s.writeClient(errPacket(2013, "08S01", fmt.Sprintf("connection %s: %v", s.p.name, err)))
				return err
			}
		}
		if err := s.replay(); err != nil {
			return err
		}
		replies, ok := s.t.command(cmd)
		if !ok {
			return fmt.Errorf("refused MySQL command 0x%02x", cmd)
		}
		switch cmd {
		case comResetConnection:
			s.setUp, s.dirty = true, false
		case comPing, comStmtClose:
		default:
			s.dirty = true
		}
		if replies {
			s.r.begin()
		}
		if _, err := s.upOut.Write(pkt); err != nil {
			return err
		}
		if in.Buffered() == 0 {
			if err := s.upOut.Flush(); err != nil {
				return err
			}
		}
	}
}

// answer answers a command without the server, when that is the same as
// what the server would answer. It is called only while the server owes
// DuckDB nothing.
func (s *mysqlSession) answer(cmd byte, pkt []byte) bool {
	open := s.up != nil && s.t.lastStatus()&statusInTrans != 0
	status := s.t.lastStatus() &^ (statusInTrans | statusStateChanged)
	switch {
	case cmd == comPing:
	case cmd == comResetConnection && !open && !s.dirty:
		s.held, s.autocommit = nil, false
	case cmd == comQuery && !open:
		switch classifyMySQL(pkt[5:]) {
		case myBegin:
			if s.held != nil {
				return false
			}
			s.held = pkt
		case myEnd:
			s.held = nil
		case myAutocommit:
			s.autocommit = true
		default:
			return false
		}
	default:
		return false
	}
	if s.held != nil {
		status |= statusInTrans
	}
	// An answer is the second packet of its exchange.
	return s.writeClient(okPacket(status, s.flags)) == nil
}

// writeClient sends DuckDB one packet answering its command.
func (s *mysqlSession) writeClient(payload []byte) error {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	b := make([]byte, 4, 4+len(payload))
	b[0], b[1], b[2], b[3] = byte(len(payload)), byte(len(payload)>>8), byte(len(payload)>>16), 1
	if _, err := s.out.Write(append(b, payload...)); err != nil {
		return err
	}
	return s.out.Flush()
}

// replay sends the server what it has not seen yet that the next command
// depends on: the session's setup after a reset, autocommit, and a held
// START TRANSACTION. Each answer is one packet, which DuckDB does not see.
func (s *mysqlSession) replay() error {
	var stmts []string
	if s.setUp {
		stmts = s.p.mysqlSetup(s.p.settings())
		s.setUp = false
	}
	if s.autocommit {
		stmts = append(stmts, "SET autocommit=1")
		s.autocommit = false
	}
	var pkts [][]byte
	for _, stmt := range stmts {
		pkts = append(pkts, append([]byte{comQuery}, stmt...))
	}
	if s.held != nil {
		pkts = append(pkts, s.held[4:])
		s.held = nil
	}
	for _, payload := range pkts {
		s.t.swallowOne()
		s.r.begin()
		b := make([]byte, 4, 4+len(payload))
		b[0], b[1], b[2], b[3] = byte(len(payload)), byte(len(payload)>>8), byte(len(payload)>>16), 0
		if _, err := s.upOut.Write(append(b, payload...)); err != nil {
			return err
		}
	}
	return nil
}

// dialMySQL connects to the remote server and reads its greeting.
func (p *proxy) dialMySQL(c Connection) (*packetConn, *mysqlGreeting, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.ConnectTimeout)
	defer cancel()
	conn, err := dial(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(c.ConnectTimeout))
	up := &packetConn{conn: conn, r: bufio.NewReader(conn), w: conn}
	pkt, _, err := up.readPacket()
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	g, err := parseGreeting(pkt)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return up, g, nil
}

// mysqlSetup are the statements that make a session read-only and limit
// its statements to query_timeout. MariaDB names the limit differently, in
// seconds.
func (p *proxy) mysqlSetup(c Connection) []string {
	stmts := []string{"SET SESSION TRANSACTION READ ONLY"}
	if c.QueryTimeout > 0 {
		g, _ := p.mysqlCache()
		if g != nil && strings.Contains(strings.ToLower(g.version), "mariadb") {
			stmts = append(stmts, fmt.Sprintf("SET SESSION max_statement_time = %.3f", c.QueryTimeout.Seconds()))
		} else {
			stmts = append(stmts, "SET SESSION max_execution_time = "+strconv.FormatInt(c.QueryTimeout.Milliseconds(), 10))
		}
	}
	return stmts
}

// signInMySQL signs in to the remote server as the connection's user, with
// the capabilities DuckDB's client chose, and makes the session read-only
// with statements limited to query_timeout. It returns the server's
// status flags.
func (p *proxy) signInMySQL(up *packetConn, g *mysqlGreeting, c Connection, flags uint32, hs *handshakeResponse) (uint16, error) {
	password, err := p.password(c)
	if err != nil {
		return 0, err
	}
	tlsCfg, err := tlsConfig(&c)
	if err != nil {
		return 0, err
	}
	useTLS := tlsCfg != nil && g.caps&capSSL != 0
	if tlsCfg != nil && !useTLS && c.TLS != TLSPrefer {
		return 0, errors.New("the server does not support TLS, which the connection's tls mode requires")
	}

	caps := flags&^(capConnectAttrs|capLocalFiles|capCompress) | capProtocol41 | capSecureConnection |
		capPluginAuth | capConnectWithDB | g.caps&(capLongPassword|capPluginAuthLenenc)
	var seq byte = 1
	if useTLS {
		caps |= capSSL
		if err := up.writePacket(seq, responseHeader(caps, hs)); err != nil {
			return 0, err
		}
		seq++
		tc := tls.Client(up.conn, tlsCfg)
		if err := tc.Handshake(); err != nil {
			return 0, fmt.Errorf("TLS: %w", err)
		}
		up.conn, up.r, up.w = tc, bufio.NewReader(tc), tc
	}

	plugin := g.plugin
	auth, err := authResponse(plugin, password, g.scramble, useTLS)
	if err != nil {
		return 0, err
	}
	resp := responseHeader(caps, hs)
	resp = append(append(resp, c.User...), 0)
	if caps&capPluginAuthLenenc != 0 {
		resp = appendLenenc(resp, uint64(len(auth)))
	} else {
		resp = append(resp, byte(len(auth)))
	}
	resp = append(resp, auth...)
	resp = append(append(resp, c.Database...), 0)
	resp = append(append(resp, plugin...), 0)
	if err := up.writePacket(seq, resp); err != nil {
		return 0, err
	}

	scramble := g.scramble
	for {
		pkt, s, err := up.readPacket()
		if err != nil {
			return 0, err
		}
		if len(pkt) == 0 {
			return 0, errors.New("empty packet while signing in")
		}
		seq = s + 1
		switch pkt[0] {
		case 0x00:
			return p.setUpMySQLSession(up, g, c)
		case 0xff:
			return 0, parseErrPacket(pkt)
		case 0xfe: // switch to another authentication method
			name, data, _ := cutNul(pkt[1:])
			plugin, scramble = string(name), bytes.TrimRight(data, "\x00")
			if auth, err = authResponse(plugin, password, scramble, useTLS); err != nil {
				return 0, err
			}
			if err := up.writePacket(seq, auth); err != nil {
				return 0, err
			}
		case 0x01: // more data for the current method
			var reply []byte
			switch {
			case plugin == "caching_sha2_password" && len(pkt) == 2 && pkt[1] == 3:
				continue // fast authentication worked; OK follows
			case plugin == "caching_sha2_password" && len(pkt) == 2 && pkt[1] == 4:
				if useTLS {
					reply = append([]byte(password), 0)
				} else {
					reply = []byte{2} // ask for the server's public key
				}
			case plugin == "caching_sha2_password" || plugin == "sha256_password":
				if reply, err = encryptPassword(password, scramble, pkt[1:]); err != nil {
					return 0, err
				}
			default:
				return 0, fmt.Errorf("unexpected data from authentication method %s", plugin)
			}
			if err := up.writePacket(seq, reply); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unexpected packet 0x%02x while signing in", pkt[0])
		}
	}
}

// setUpMySQLSession makes the session read-only and limits its statements
// to query_timeout, and remembers the server's greeting and status for
// sessions to come.
func (p *proxy) setUpMySQLSession(up *packetConn, g *mysqlGreeting, c Connection) (uint16, error) {
	p.mu.Lock()
	p.greeting = g
	p.mu.Unlock()
	var status uint16
	for i, stmt := range p.mysqlSetup(c) {
		if err := up.writePacket(0, append([]byte{comQuery}, stmt...)); err != nil {
			return 0, err
		}
		pkt, _, err := up.readPacket()
		if err != nil {
			return 0, err
		}
		switch {
		case len(pkt) > 0 && pkt[0] == 0x00:
			status = okStatus(pkt)
		case i == 0:
			return 0, fmt.Errorf("make the session read-only: %w", parseErrPacket(pkt))
		default:
			// An older server without statement limits; Igloo's own
			// timeout still applies.
			p.log.Debug("could not limit statement time on the remote database", "err", parseErrPacket(pkt))
		}
	}
	_ = up.conn.SetDeadline(time.Time{})
	status &^= statusStateChanged | statusInTrans
	p.mu.Lock()
	p.status = status
	p.mu.Unlock()
	return status, nil
}

// errLocalInfile ends a session whose server asked for a local file.
var errLocalInfile = errors.New("the remote server asked for a local file (LOAD DATA LOCAL INFILE); refused")

// fromServer relays the server's packets to DuckDB, following the shape of
// each answer, but for the answers to what replay sent, which must all
// succeed.
func (s *mysqlSession) fromServer(in *bufio.Reader) error {
	continued := false
	for {
		n, header, err := readHeader(in)
		if err != nil {
			return err
		}
		if !continued && s.t.swallowing() {
			head, err := in.Peek(min(n, 1))
			if err != nil {
				return err
			}
			if _, err := in.Discard(n); err != nil {
				return err
			}
			if len(head) == 0 || head[0] != 0x00 {
				s.p.log.Warn("closed a session whose setup failed on the remote database")
				return errors.New("the remote database refused the session's setup")
			}
			s.t.swallowed()
			s.r.end()
			continue
		}
		done := false
		if !continued {
			head, err := in.Peek(min(n, 32))
			if err != nil {
				return err
			}
			if done, err = s.t.reply(head, n); err != nil {
				s.p.log.Warn("closed a session with the remote database", "err", err)
				return err
			}
		}
		continued = n == maxPacketPayload
		s.outMu.Lock()
		_, err = s.out.Write(header[:])
		if err == nil {
			_, err = io.CopyN(s.out, in, int64(n))
		}
		if err == nil && in.Buffered() == 0 {
			err = s.out.Flush()
		}
		s.outMu.Unlock()
		if err != nil {
			return err
		}
		if done {
			s.r.end()
		} else {
			s.r.progress()
		}
	}
}

// mysqlTracker follows which command's answer the server is sending and
// where in it it is. It errs only towards keeping the session alive: a
// wrong guess at where an answer ends can only clear or keep the silence
// deadline, while the check for local-file requests applies to the first
// packet of every result of a query, where MySQL places them.
type mysqlTracker struct {
	deprecateEOF bool

	mu        sync.Mutex
	cmd       byte
	phase     int
	remaining int
	// columns follow a prepared statement's parameters.
	columns int
	// status is the server's status flags as last reported.
	status uint16
	// swallow counts single-packet answers DuckDB does not see.
	swallow int
}

func (t *mysqlTracker) lastStatus() uint16 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

func (t *mysqlTracker) swallowOne() {
	t.mu.Lock()
	t.swallow++
	t.mu.Unlock()
}

func (t *mysqlTracker) swallowing() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.swallow > 0
}

func (t *mysqlTracker) swallowed() {
	t.mu.Lock()
	t.swallow--
	t.mu.Unlock()
}

const (
	phaseIdle = iota
	phaseFirst
	phaseColumns
	phaseColumnsEOF
	phaseRows
	phasePrepare
	phaseParams
	phaseParamsEOF
	phasePrepColumns
	phasePrepColumnsEOF
	phaseFieldList
	phaseSingle
)

// command notes a command from the client. replies says whether the
// server answers it; ok is false for commands the proxy refuses.
func (t *mysqlTracker) command(cmd byte) (replies, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cmd = cmd
	switch cmd {
	case comQuery, comStmtExecute:
		t.phase = phaseFirst
	case comStmtPrepare:
		t.phase = phasePrepare
	case comStmtFetch:
		t.phase = phaseRows
	case comFieldList:
		t.phase = phaseFieldList
	case comPing, comStmtReset, comInitDB, comResetConnection, comSetOption, comStatistics:
		t.phase = phaseSingle
	case comStmtClose, comStmtSendLongData, comQuit:
		t.phase = phaseIdle
		return false, true
	default:
		return false, false
	}
	return true, true
}

// reply classifies a packet of the server's answer: head is its start, n
// its length. done reports that the answer is complete.
func (t *mysqlTracker) reply(head []byte, n int) (done bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(head) == 0 {
		return false, nil
	}
	h := head[0]
	terminator := h == 0xfe && (t.deprecateEOF && n < maxPacketPayload || !t.deprecateEOF && n < 9)
	switch t.phase {
	case phaseFirst:
		switch {
		case h == 0xfb && t.cmd == comQuery:
			return false, errLocalInfile
		case h == 0x00:
			return t.endUnlessMore(okStatus(head)), nil
		case h == 0xff:
			return t.finish(), nil
		}
		cols, ok := readLenenc(head)
		if !ok || cols == 0 {
			return t.finish(), nil
		}
		t.phase, t.remaining = phaseColumns, int(cols)
	case phaseColumns:
		if t.remaining--; t.remaining <= 0 {
			t.phase = phaseRows
			if !t.deprecateEOF {
				t.phase = phaseColumnsEOF
			}
		}
	case phaseColumnsEOF:
		if t.cmd == comStmtExecute && eofStatus(head)&statusCursorExists != 0 {
			return t.finish(), nil
		}
		t.phase = phaseRows
	case phaseRows:
		switch {
		case h == 0xff:
			return t.finish(), nil
		case terminator && t.deprecateEOF:
			return t.endUnlessMore(okStatus(head)), nil
		case terminator:
			return t.endUnlessMore(eofStatus(head)), nil
		}
	case phasePrepare:
		if h != 0x00 || len(head) < 9 {
			return t.finish(), nil
		}
		t.columns = int(binary.LittleEndian.Uint16(head[5:7]))
		t.remaining = int(binary.LittleEndian.Uint16(head[7:9]))
		return t.nextPrepare(phasePrepare), nil
	case phaseParams:
		if t.remaining--; t.remaining <= 0 {
			return t.nextPrepare(phaseParams), nil
		}
	case phaseParamsEOF:
		return t.nextPrepare(phaseParamsEOF), nil
	case phasePrepColumns:
		if t.remaining--; t.remaining <= 0 {
			return t.nextPrepare(phasePrepColumns), nil
		}
	case phasePrepColumnsEOF:
		return t.finish(), nil
	case phaseFieldList:
		if h == 0xff || h == 0xfe && n < 9 {
			return t.finish(), nil
		}
	case phaseSingle:
		return t.finish(), nil
	}
	return false, nil
}

// nextPrepare moves through a prepared statement's answer: its parameter
// definitions, then its column definitions, each followed by EOF unless
// the client deprecated it. It reports whether the answer is complete.
func (t *mysqlTracker) nextPrepare(from int) bool {
	switch from {
	case phasePrepare:
		if t.remaining > 0 {
			t.phase = phaseParams
			return false
		}
	case phaseParams:
		if !t.deprecateEOF {
			t.phase = phaseParamsEOF
			return false
		}
	}
	if from != phasePrepColumns && t.columns > 0 {
		t.phase, t.remaining, t.columns = phasePrepColumns, t.columns, 0
		return false
	}
	if from == phasePrepColumns && !t.deprecateEOF {
		t.phase = phasePrepColumnsEOF
		return false
	}
	return t.finish()
}

// endUnlessMore ends the answer unless the server says another result
// follows, whose first packet is checked like the first one's.
func (t *mysqlTracker) endUnlessMore(status uint16) bool {
	t.status = status
	if status&statusMoreResults != 0 {
		t.phase = phaseFirst
		return false
	}
	return t.finish()
}

func (t *mysqlTracker) finish() bool {
	t.phase = phaseIdle
	return true
}

// --- packets ----------------------------------------------------------------

// packetConn reads and writes MySQL packets while signing in.
type packetConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    io.Writer
}

func readHeader(r *bufio.Reader) (int, [4]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, h, err
	}
	return int(h[0]) | int(h[1])<<8 | int(h[2])<<16, h, nil
}

// readPacket reads one packet of at most 64 KiB, enough for anything sent
// while signing in.
func (c *packetConn) readPacket() ([]byte, byte, error) {
	n, h, err := readHeader(c.r)
	if err != nil {
		return nil, 0, err
	}
	if n > 64<<10 {
		return nil, 0, errors.New("packet too large while signing in")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(c.r, b); err != nil {
		return nil, 0, err
	}
	return b, h[3], nil
}

func (c *packetConn) writePacket(seq byte, payload []byte) error {
	b := make([]byte, 4, 4+len(payload))
	b[0], b[1], b[2], b[3] = byte(len(payload)), byte(len(payload)>>8), byte(len(payload)>>16), seq
	_, err := c.w.Write(append(b, payload...))
	return err
}

// mysqlGreeting is what a server says first (HandshakeV10).
type mysqlGreeting struct {
	version  string
	connID   uint32
	scramble []byte
	caps     uint32
	charset  byte
	status   uint16
	plugin   string
}

func parseGreeting(p []byte) (*mysqlGreeting, error) {
	if len(p) > 0 && p[0] == 0xff {
		return nil, parseErrPacket(p)
	}
	if len(p) < 1 || p[0] != 10 {
		return nil, errors.New("not a MySQL server, or an unsupported protocol version")
	}
	version, rest, ok := cutNul(p[1:])
	if !ok || len(rest) < 4+8+1+2 {
		return nil, errors.New("malformed server greeting")
	}
	g := &mysqlGreeting{version: string(version), connID: binary.LittleEndian.Uint32(rest)}
	g.scramble = append([]byte(nil), rest[4:12]...)
	g.caps = uint32(binary.LittleEndian.Uint16(rest[13:15]))
	rest = rest[15:]
	authLen := 0
	if len(rest) >= 16 {
		g.charset = rest[0]
		g.status = binary.LittleEndian.Uint16(rest[1:3])
		g.caps |= uint32(binary.LittleEndian.Uint16(rest[3:5])) << 16
		authLen = int(rest[5])
		rest = rest[16:]
	}
	if g.caps&capProtocol41 == 0 || g.caps&capSecureConnection == 0 {
		return nil, errors.New("the server is too old (it lacks the 4.1 protocol)")
	}
	n := max(13, authLen-8)
	if len(rest) < n {
		return nil, errors.New("malformed server greeting")
	}
	g.scramble = append(g.scramble, bytes.TrimRight(rest[:n], "\x00")...)
	rest = rest[n:]
	g.plugin = nativePasswordPlugin
	if g.caps&capPluginAuth != 0 {
		name, _, _ := cutNul(rest)
		g.plugin = string(name)
	}
	return g, nil
}

// buildGreeting greets DuckDB's client with the remote server's version,
// connection ID, character set and status, and the proxy's own scramble
// and capabilities. MariaDB's extended capabilities are offered as none.
func buildGreeting(g *mysqlGreeting, scramble []byte, caps uint32) []byte {
	b := append([]byte{10}, g.version...)
	b = append(b, 0)
	b = binary.LittleEndian.AppendUint32(b, g.connID)
	b = append(b, scramble[:8]...)
	b = append(b, 0)
	b = binary.LittleEndian.AppendUint16(b, uint16(caps))
	b = append(b, g.charset)
	b = binary.LittleEndian.AppendUint16(b, g.status)
	b = binary.LittleEndian.AppendUint16(b, uint16(caps>>16))
	b = append(b, byte(len(scramble)+1))
	b = append(b, make([]byte, 10)...)
	b = append(b, scramble[8:]...)
	b = append(b, 0)
	return append(append(b, nativePasswordPlugin...), 0)
}

// handshakeResponse is what DuckDB's client answers the greeting with.
type handshakeResponse struct {
	caps      uint32
	maxPacket uint32
	charset   byte
	auth      []byte
	plugin    string
}

func parseHandshakeResponse(p []byte) (*handshakeResponse, error) {
	if len(p) < 32 {
		return nil, errors.New("malformed handshake response")
	}
	h := &handshakeResponse{caps: binary.LittleEndian.Uint32(p), maxPacket: binary.LittleEndian.Uint32(p[4:]), charset: p[8]}
	if h.caps&capProtocol41 == 0 {
		return nil, errors.New("the client lacks the 4.1 protocol")
	}
	if h.caps&capSSL != 0 {
		return nil, errors.New("TLS was not offered")
	}
	_, rest, ok := cutNul(p[32:]) // the user, which the token stands for
	if !ok {
		return nil, errors.New("malformed handshake response")
	}
	switch {
	case h.caps&capPluginAuthLenenc != 0:
		n, size := lenenc(rest)
		if size == 0 || uint64(len(rest)-size) < n {
			return nil, errors.New("malformed handshake response")
		}
		h.auth, rest = rest[size:size+int(n)], rest[size+int(n):]
	default:
		if len(rest) < 1 || len(rest) < 1+int(rest[0]) {
			return nil, errors.New("malformed handshake response")
		}
		h.auth, rest = rest[1:1+int(rest[0])], rest[1+int(rest[0]):]
	}
	if h.caps&capConnectWithDB != 0 {
		_, rest, _ = cutNul(rest)
	}
	h.plugin = nativePasswordPlugin
	if h.caps&capPluginAuth != 0 {
		name, _, _ := cutNul(rest)
		h.plugin = string(name)
	}
	return h, nil
}

// responseHeader is the fixed start of a handshake response, which is also
// the whole of a TLS request.
func responseHeader(caps uint32, hs *handshakeResponse) []byte {
	b := binary.LittleEndian.AppendUint32(nil, caps)
	b = binary.LittleEndian.AppendUint32(b, hs.maxPacket)
	b = append(b, hs.charset)
	return append(b, make([]byte, 23)...)
}

func authSwitch(plugin string, scramble []byte) []byte {
	b := append([]byte{0xfe}, plugin...)
	b = append(b, 0)
	return append(append(b, scramble...), 0)
}

// okPacket tells DuckDB's client it is signed in.
func okPacket(status uint16, caps uint32) []byte {
	b := []byte{0, 0, 0}
	b = binary.LittleEndian.AppendUint16(b, status)
	b = append(b, 0, 0)
	if caps&capSessionTrack != 0 {
		b = append(b, 0) // no info
	}
	return b
}

func errPacket(code uint16, state, msg string) []byte {
	b := binary.LittleEndian.AppendUint16([]byte{0xff}, code)
	b = append(b, '#')
	b = append(b, state...)
	return append(b, Scrub(msg)...)
}

func parseErrPacket(p []byte) error {
	if len(p) < 3 || p[0] != 0xff {
		return errors.New("unexpected answer from the server")
	}
	code := binary.LittleEndian.Uint16(p[1:3])
	msg := p[3:]
	if len(msg) >= 6 && msg[0] == '#' {
		msg = msg[6:]
	}
	return fmt.Errorf("MySQL error %d: %s", code, msg)
}

// okStatus returns the status flags of an OK packet (header 0x00 or 0xfe).
func okStatus(p []byte) uint16 {
	if len(p) < 1 {
		return 0
	}
	rest := p[1:]
	for range 2 { // affected rows, last insert ID
		_, size := lenenc(rest)
		if size == 0 {
			return 0
		}
		rest = rest[size:]
	}
	if len(rest) < 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(rest)
}

// eofStatus returns the status flags of an EOF packet.
func eofStatus(p []byte) uint16 {
	if len(p) < 5 {
		return 0
	}
	return binary.LittleEndian.Uint16(p[3:5])
}

// lenenc decodes a length-encoded integer, returning it and its size, or
// size 0 if it is malformed or cut off.
func lenenc(b []byte) (uint64, int) {
	if len(b) == 0 {
		return 0, 0
	}
	switch b[0] {
	case 0xfc:
		if len(b) >= 3 {
			return uint64(binary.LittleEndian.Uint16(b[1:])), 3
		}
	case 0xfd:
		if len(b) >= 4 {
			return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4
		}
	case 0xfe:
		if len(b) >= 9 {
			return binary.LittleEndian.Uint64(b[1:]), 9
		}
	case 0xfb, 0xff:
	default:
		return uint64(b[0]), 1
	}
	return 0, 0
}

func readLenenc(b []byte) (uint64, bool) {
	n, size := lenenc(b)
	return n, size > 0
}

func appendLenenc(b []byte, n uint64) []byte {
	switch {
	case n < 251:
		return append(b, byte(n))
	case n < 1<<16:
		return binary.LittleEndian.AppendUint16(append(b, 0xfc), uint16(n))
	case n < 1<<24:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	return binary.LittleEndian.AppendUint64(append(b, 0xfe), n)
}

func cutNul(b []byte) (before, after []byte, ok bool) {
	return bytes.Cut(b, []byte{0})
}

// newScramble returns 20 printable random bytes, as servers send.
func newScramble() []byte {
	b := make([]byte, 20)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = 0x21 + b[i]%(0x7e-0x21)
	}
	return b
}

// --- authentication ---------------------------------------------------------

// authResponse answers an authentication method's challenge.
func authResponse(plugin, password string, scramble []byte, overTLS bool) ([]byte, error) {
	switch plugin {
	case nativePasswordPlugin, "":
		return nativePassword(password, scramble), nil
	case "caching_sha2_password":
		return cachingSHA2Password(password, scramble), nil
	case "sha256_password":
		if password == "" {
			return []byte{0}, nil
		}
		if overTLS {
			return append([]byte(password), 0), nil
		}
		return []byte{1}, nil // ask for the server's public key
	case "mysql_clear_password":
		if !overTLS {
			return nil, errors.New("the server asks for the password in clear text; use a tls mode that encrypts")
		}
		return append([]byte(password), 0), nil
	}
	return nil, fmt.Errorf("unsupported authentication method %s; use mysql_native_password, caching_sha2_password or sha256_password", plugin)
}

// nativePassword is SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password))).
func nativePassword(password string, scramble []byte) []byte {
	if password == "" {
		return nil
	}
	s1 := sha1.Sum([]byte(password)) //nolint:gosec
	s2 := sha1.Sum(s1[:])            //nolint:gosec
	h := sha1.New()                  //nolint:gosec
	h.Write(scramble[:min(20, len(scramble))])
	h.Write(s2[:])
	out := h.Sum(nil)
	for i := range out {
		out[i] ^= s1[i]
	}
	return out
}

// cachingSHA2Password is SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble).
func cachingSHA2Password(password string, scramble []byte) []byte {
	if password == "" {
		return nil
	}
	s1 := sha256.Sum256([]byte(password))
	s2 := sha256.Sum256(s1[:])
	h := sha256.New()
	h.Write(s2[:])
	h.Write(scramble[:min(20, len(scramble))])
	out := h.Sum(nil)
	for i := range out {
		out[i] ^= s1[i]
	}
	return out
}

// encryptPassword encrypts the password for a server that sent its public
// key, as caching_sha2_password and sha256_password do without TLS.
func encryptPassword(password string, scramble, keyPEM []byte) ([]byte, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, errors.New("the server sent no public key")
	}
	var pub *rsa.PublicKey
	if k, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		pub, _ = k.(*rsa.PublicKey)
	} else if k, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		pub = k
	}
	if pub == nil {
		return nil, errors.New("the server's public key is not an RSA key")
	}
	plain := append([]byte(password), 0)
	for i := range plain {
		plain[i] ^= scramble[i%len(scramble)]
	}
	return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, plain, nil) //nolint:gosec
}
