package pgwire

import (
	"testing"

	"github.com/akkio-inc/igloo/internal/federation"
)

func TestSnowflakeConnectionStatement(t *testing.T) {
	t.Parallel()
	c, ok, err := parseConnectionCommand(`CREATE CONNECTION wh TYPE snowflake WITH (account 'myorg-acct', user 'IGLOO',
		private_key_secret 'wh-key', warehouse = 'XS_WH', role 'READER', database 'ANALYTICS', schema 'PUBLIC', query_timeout '2m')`)
	if err != nil || !ok || c.typ != federation.Snowflake || len(c.options) != 8 {
		t.Fatalf("%+v %v %v", c, ok, err)
	}
	conn := federation.New(c.name, c.typ)
	for _, o := range c.options {
		if err := conn.Set(o[0], o[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Validate(nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parseConnectionCommand("CREATE CONNECTION wh TYPE bigquery WITH (project 'p')"); err == nil {
		t.Error("an unsupported type parsed")
	}
}
