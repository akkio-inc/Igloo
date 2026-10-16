package federation

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
)

func TestConnectionOptions(t *testing.T) {
	c := New("crm", Postgres)
	for k, v := range map[string]string{
		"HOST": "db.example.com", "port": "6543", "dbname": "crm", "user": "reader", "password": "pw",
		"sslmode": "verify-full", "schema": "public, sales", "connect_timeout": "3s", "query_timeout": "90",
		"read_only": "true",
	} {
		if err := c.Set(k, v); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	if c.Host != "db.example.com" || c.Port != 6543 || c.Database != "crm" || c.User != "reader" || c.Password != "pw" ||
		c.TLS != TLSVerifyFull || strings.Join(c.Schemas, "|") != "public|sales" ||
		c.ConnectTimeout != 3*time.Second || c.QueryTimeout != 90*time.Second {
		t.Errorf("%+v", c)
	}
	if err := c.Validate([]string{"lake", "memory"}); err != nil {
		t.Error(err)
	}
	// A secret reference replaces a stored password, and the other way
	// round.
	if err := c.Set("password_secret", "crm-password"); err != nil || c.Password != "" || c.PasswordSource() != "secret crm-password" {
		t.Errorf("password_secret: %v %+v", err, c)
	}
	if err := c.Set("password", "x"); err != nil || c.PasswordSecret != "" || c.PasswordSource() != "stored" {
		t.Errorf("password: %v %+v", err, c)
	}
	if m := New("ops", MySQL); m.Port != 3306 || m.TLS != TLSPrefer {
		t.Errorf("MySQL defaults: %+v", m)
	}
	if err := c.Set("tls", "verify_identity"); err != nil || c.TLS != TLSVerifyFull {
		t.Errorf("MySQL's ssl_mode names: %v", err)
	}

	for _, bad := range [][2]string{
		{"read_only", "false"}, {"port", "70000"}, {"port", "x"}, {"tls", "maybe"},
		{"query_timeout", "-1s"}, {"secret", "x"}, {"connect_timeout", "soon"},
	} {
		if err := c.Set(bad[0], bad[1]); err == nil {
			t.Errorf("%s = %s accepted", bad[0], bad[1])
		}
	}

	valid := func() Connection {
		c := New("crm", Postgres)
		c.Host, c.Database, c.User = "db", "crm", "u"
		return c
	}
	for name, change := range map[string]func(*Connection){
		"reserved name":     func(c *Connection) { c.Name = "system" },
		"taken name":        func(c *Connection) { c.Name = "lake" },
		"upper case name":   func(c *Connection) { c.Name = "CRM" },
		"no host":           func(c *Connection) { c.Host = "" },
		"host with spaces":  func(c *Connection) { c.Host = "db host=evil" },
		"no database":       func(c *Connection) { c.Database = "" },
		"no user":           func(c *Connection) { c.User = "" },
		"secret with slash": func(c *Connection) { c.PasswordSecret = "../etc/passwd" },
		"secret dot dot":    func(c *Connection) { c.PasswordSecret = ".." },
		"zero timeout":      func(c *Connection) { c.ConnectTimeout = 0 },
		"bad CA":            func(c *Connection) { c.TLSCA = "not a certificate" },
		"CA without TLS":    func(c *Connection) { c.TLS, c.TLSCA = TLSDisable, "x" },
		"control character": func(c *Connection) { c.User = "a\x00b" },
	} {
		c := valid()
		change(&c)
		if err := c.Validate([]string{"lake"}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := ParseType("MariaDB"); err != nil {
		t.Error(err)
	}
	if _, err := ParseType("oracle"); err == nil {
		t.Error("oracle accepted")
	}
}

func TestSealer(t *testing.T) {
	s, err := newSealer(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	sealed := s.seal("crm", "hunter2")
	if bytes.Contains(sealed, []byte("hunter2")) {
		t.Fatal("the password is stored in clear")
	}
	if got, err := s.open("crm", sealed); err != nil || got != "hunter2" {
		t.Errorf("open: %q %v", got, err)
	}
	if _, err := s.open("ops", sealed); err == nil {
		t.Error("a password sealed for one connection opened for another")
	}
	other, _ := newSealer(bytes.Repeat([]byte{8}, 32))
	if _, err := other.open("crm", sealed); err == nil {
		t.Error("another key opened the password")
	}
	if _, err := newSealer([]byte("short")); err == nil {
		t.Error("a short key was accepted")
	}
}

func TestScrub(t *testing.T) {
	tok := newToken()
	if got := Scrub("password=" + tok + " host=x"); strings.Contains(got, tok) || !strings.Contains(got, "***") {
		t.Errorf("Scrub: %q", got)
	}
}

func TestAttachStatements(t *testing.T) {
	c := New("crm", Postgres)
	c.Database, c.Schemas = "crm", []string{"public", "o'brien"}
	got := strings.Join(attachStatements(c, "tok", 4321), "\n")
	for _, want := range []string{
		`CREATE OR REPLACE SECRET "igloo_connection_crm" (TYPE postgres, PASSWORD 'tok')`,
		`ATTACH 'host=127.0.0.1 port=4321 user=igloo dbname=crm sslmode=disable' AS "crm" (TYPE postgres, SECRET "igloo_connection_crm", READ_ONLY, SCHEMA ['public', 'o''brien'])`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in\n%s", want, got)
		}
	}
	m := New("ops", MySQL)
	m.Database = `we"ird`
	got = strings.Join(attachStatements(m, "tok", 4321), "\n")
	if !strings.Contains(got, `database="we\"ird" ssl_mode=disabled' AS "ops" (TYPE mysql`) {
		t.Errorf("MySQL: %s", got)
	}
}

// TestMySQLTracker follows the answers MySQL servers send, and refuses a
// request for a local file wherever a result can start.
func TestMySQLTracker(t *testing.T) {
	ok := []byte{0x00, 0, 0, 0x02, 0x00, 0, 0}
	okMore := []byte{0x00, 0, 0, 0x0a, 0x00, 0, 0}
	eof := []byte{0xfe, 0, 0, 0x02, 0x00}
	eofMore := []byte{0xfe, 0, 0, 0x0a, 0x00}
	errPkt := []byte{0xff, 0x10, 0x04, '#', 'H', 'Y', '0', '0', '0', 'x'}
	col := []byte{0x03, 'd', 'e', 'f'}
	row := []byte{0x01, 'x'}
	nullRow := []byte{0xfb}
	infile := []byte{0xfb, '/', 'e', 't', 'c'}

	type step struct {
		client byte
		reply  []byte
		done   bool
		bad    bool
	}
	run := func(name string, deprecateEOF bool, steps []step) {
		t.Helper()
		tr := &mysqlTracker{deprecateEOF: deprecateEOF}
		for i, s := range steps {
			if s.client != 0 {
				if replies, ok := tr.command(s.client); !ok || !replies {
					t.Errorf("%s: step %d: command 0x%x: %v %v", name, i, s.client, replies, ok)
				}
				continue
			}
			done, err := tr.reply(s.reply, len(s.reply))
			if (err != nil) != s.bad || done != s.done {
				t.Errorf("%s: step %d: done %v err %v, want done %v bad %v", name, i, done, err, s.done, s.bad)
			}
		}
	}
	run("OK", false, []step{{client: comQuery}, {reply: ok, done: true}})
	run("error", false, []step{{client: comQuery}, {reply: errPkt, done: true}})
	run("rows with EOF", false, []step{{client: comQuery}, {reply: []byte{2}}, {reply: col}, {reply: col}, {reply: eof},
		{reply: row}, {reply: nullRow}, {reply: eof, done: true}})
	run("rows without EOF", true, []step{{client: comQuery}, {reply: []byte{1}}, {reply: col}, {reply: nullRow},
		{reply: ok[:1:1]}, {reply: []byte{0xfe, 0, 0, 0x02, 0, 0, 0}, done: true}})
	run("local infile", false, []step{{client: comQuery}, {reply: infile, bad: true}})
	run("local infile in a later result", false, []step{{client: comQuery}, {reply: okMore}, {reply: infile, bad: true}})
	run("local infile after rows", false, []step{{client: comQuery}, {reply: []byte{1}}, {reply: col}, {reply: eof},
		{reply: row}, {reply: eofMore}, {reply: infile, bad: true}})
	run("prepare", false, []step{{client: comStmtPrepare},
		{reply: []byte{0, 1, 0, 0, 0, 2, 0, 1, 0, 0, 0, 0}}, {reply: col}, {reply: eof}, {reply: col}, {reply: col}, {reply: eof, done: true}})
	run("prepare without EOF", true, []step{{client: comStmtPrepare},
		{reply: []byte{0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0}}, {reply: col, done: true}})
	run("prepare with nothing", false, []step{{client: comStmtPrepare}, {reply: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, done: true}})
	run("execute", false, []step{{client: comStmtExecute}, {reply: []byte{1}}, {reply: col}, {reply: eof},
		{reply: []byte{0x00, 0x00, 1}}, {reply: eof, done: true}})
	run("ping", false, []step{{client: comPing}, {reply: ok, done: true}})

	tr := &mysqlTracker{}
	for _, cmd := range []byte{comStmtClose, comQuit, comStmtSendLongData} {
		if replies, ok := tr.command(cmd); replies || !ok {
			t.Errorf("command 0x%x: %v %v", cmd, replies, ok)
		}
	}
	for _, cmd := range []byte{0x11 /* change user */, 0x12 /* binlog dump */, 0x0c /* kill */} {
		if _, ok := tr.command(cmd); ok {
			t.Errorf("command 0x%x allowed", cmd)
		}
	}
}

// TestRelaySilence gives up on a remote database that leaves a statement
// unanswered, and not on one that is idle.
func TestRelaySilence(t *testing.T) {
	client, clientEnd := net.Pipe()
	upstream, upstreamEnd := net.Pipe()
	defer clientEnd.Close()
	defer upstreamEnd.Close()
	r := newRelay(client, time.Millisecond)
	r.setUpstream(upstream)
	r.timeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := upstream.Read(buf)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("an idle session timed out: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	r.begin()
	select {
	case err := <-done:
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Errorf("want a timeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a silent remote database was not given up on")
	}
}

func TestMySQLPasswords(t *testing.T) {
	scramble := []byte("12345678901234567890")
	if len(nativePassword("", scramble)) != 0 || len(nativePassword("pw", scramble)) != 20 {
		t.Error("mysql_native_password")
	}
	if len(cachingSHA2Password("pw", scramble)) != 32 {
		t.Error("caching_sha2_password")
	}
	if _, err := authResponse("mysql_clear_password", "pw", scramble, false); err == nil {
		t.Error("a clear-text password without TLS")
	}
	if _, err := authResponse("auth_gssapi_client", "pw", scramble, true); err == nil {
		t.Error("an unsupported method was accepted")
	}
	g := &mysqlGreeting{version: "8.4.0", connID: 7, caps: capProtocol41 | capSecureConnection | capPluginAuth, charset: 45, status: 2}
	parsed, err := parseGreeting(buildGreeting(g, scramble, g.caps))
	if err != nil || parsed.version != "8.4.0" || !bytes.Equal(parsed.scramble, scramble) || parsed.plugin != nativePasswordPlugin ||
		parsed.connID != 7 {
		t.Errorf("greeting round trip: %+v %v", parsed, err)
	}
}
