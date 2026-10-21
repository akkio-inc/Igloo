package snowflake

import "testing"

// TestOutputInSessionTimeZone checks that TO_VARCHAR and TO_CHAR write a
// timestamp with a time zone in the session's time zone, with the offset
// as Snowflake writes it.
func TestOutputInSessionTimeZone(t *testing.T) {
	conn := session(t)
	const at = `'2026-10-07 15:58:37.862 +00:00'::timestamp_ltz`
	for _, c := range []runCase{
		{sql: `alter session set timezone = 'America/Los_Angeles'`},
		// What Snowflake returned for Acme's column, in its default time zone.
		{name: "los angeles", sql: `select to_varchar(` + at + `), to_varchar('2026-01-07 15:58:37.862 +00:00'::timestamp_ltz), to_char(` + at + `, 'HH24:MI TZH:TZM'), ` + at + `::string`,
			want: []string{"2026-10-07 08:58:37.862 -0700|2026-01-07 07:58:37.862 -0800|08:58 -07:00|2026-10-07 08:58:37.862 -0700"}},
		{sql: `alter session set timezone = 'Asia/Kolkata'`},
		{name: "kolkata", sql: `select to_varchar(` + at + `), to_char(` + at + `, 'TZHTZM')`,
			want: []string{"2026-10-07 21:28:37.862 +0530|+0530"}},
		{sql: `alter session set timezone = 'America/St_Johns'`},
		{name: "st johns", sql: `select to_varchar(` + at + `), to_char(` + at + `, 'TZH:TZM')`,
			want: []string{"2026-10-07 13:28:37.862 -0230|-02:30"}},
		// A TIMESTAMP_NTZ has no time zone to show.
		{name: "ntz", sql: `select to_varchar('2026-10-07 15:58:37.862'::timestamp_ntz)`, want: []string{"2026-10-07 15:58:37.862"}},
	} {
		run(t, conn, c)
	}
}
