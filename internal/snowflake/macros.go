package snowflake

// macros define, for one session, the DuckDB macros that translated
// statements call. Their names start with sf_, which no DuckDB function's
// does, so they shadow nothing. Every session gets them, whatever its
// dialect, since views, tasks and the like created in Snowflake's dialect
// keep calling them. Each is an expression over its arguments: none
// reads a table, a file or session state. Several have an overload per
// argument type, which DuckDB chooses when it binds the call, to keep the
// type of a date or timestamp as Snowflake does (DuckDB's DATE + INTERVAL
// is a TIMESTAMP).
var macros = []string{
	// DATEADD by months (factor 12 for years, 3 for quarters), days
	// (factor 7 for weeks) and microseconds (any smaller part). Snowflake
	// rounds the amount to a whole number first.
	`CREATE OR REPLACE TEMP MACRO sf_add_months(x DATE, n, f) AS CAST(x + to_months(CAST(n AS INTEGER) * f) AS DATE),
		(x TIMESTAMP, n, f) AS x + to_months(CAST(n AS INTEGER) * f),
		(x TIMESTAMPTZ, n, f) AS x + to_months(CAST(n AS INTEGER) * f),
		(x VARCHAR, n, f) AS CAST(x AS TIMESTAMP) + to_months(CAST(n AS INTEGER) * f)`,
	`CREATE OR REPLACE TEMP MACRO sf_add_days(x DATE, n, f) AS x + CAST(CAST(n AS INTEGER) * f AS INTEGER),
		(x TIMESTAMP, n, f) AS x + to_days(CAST(CAST(n AS INTEGER) * f AS INTEGER)),
		(x TIMESTAMPTZ, n, f) AS x + to_days(CAST(CAST(n AS INTEGER) * f AS INTEGER)),
		(x VARCHAR, n, f) AS CAST(x AS TIMESTAMP) + to_days(CAST(CAST(n AS INTEGER) * f AS INTEGER))`,
	`CREATE OR REPLACE TEMP MACRO sf_add_micros(x DATE, n, f) AS CAST(x AS TIMESTAMP) + to_microseconds(CAST(CAST(n AS BIGINT) * f AS BIGINT)),
		(x TIMESTAMP, n, f) AS x + to_microseconds(CAST(CAST(n AS BIGINT) * f AS BIGINT)),
		(x TIMESTAMPTZ, n, f) AS x + to_microseconds(CAST(CAST(n AS BIGINT) * f AS BIGINT)),
		(x TIME, n, f) AS x + to_microseconds(CAST(CAST(n AS BIGINT) * f AS BIGINT)),
		(x VARCHAR, n, f) AS CAST(x AS TIMESTAMP) + to_microseconds(CAST(CAST(n AS BIGINT) * f AS BIGINT))`,
	// DATE_TRUNC, which keeps a DATE a DATE and a TIME a TIME.
	`CREATE OR REPLACE TEMP MACRO sf_date_trunc(p, x DATE) AS CAST(date_trunc(p, x) AS DATE),
		(p, x TIMESTAMP) AS date_trunc(p, x),
		(p, x TIMESTAMPTZ) AS date_trunc(p, x),
		(p, x TIME) AS CAST(date_trunc(p, DATE '1970-01-01' + x) AS TIME),
		(p, x VARCHAR) AS date_trunc(p, CAST(x AS TIMESTAMP))`,
	// TIME_SLICE: buckets of width w counted from origin o.
	`CREATE OR REPLACE TEMP MACRO sf_time_slice(x DATE, w, o) AS time_bucket(w, x, CAST(o AS DATE)),
		(x TIMESTAMP, w, o) AS time_bucket(w, x, CAST(o AS TIMESTAMP)),
		(x TIMESTAMPTZ, w, o) AS time_bucket(w, x, CAST(o AS TIMESTAMPTZ)),
		(x VARCHAR, w, o) AS time_bucket(w, CAST(x AS TIMESTAMP), CAST(o AS TIMESTAMP))`,
	// ADD_MONTHS, which keeps the last day of a month the last day.
	`CREATE OR REPLACE TEMP MACRO sf_add_months_eom(x DATE, n) AS
			CASE WHEN x = last_day(x) THEN last_day(x + to_months(CAST(n AS INTEGER))) ELSE CAST(x + to_months(CAST(n AS INTEGER)) AS DATE) END,
		(x TIMESTAMP, n) AS
			CASE WHEN CAST(x AS DATE) = last_day(x) THEN CAST(last_day(x + to_months(CAST(n AS INTEGER))) AS TIMESTAMP) + (x - date_trunc('day', x))
			ELSE x + to_months(CAST(n AS INTEGER)) END,
		(x TIMESTAMPTZ, n) AS
			CASE WHEN CAST(x AS DATE) = last_day(x) THEN CAST(last_day(x + to_months(CAST(n AS INTEGER))) AS TIMESTAMPTZ) + (x - date_trunc('day', x))
			ELSE x + to_months(CAST(n AS INTEGER)) END,
		(x VARCHAR, n) AS
			CASE WHEN CAST(CAST(x AS TIMESTAMP) AS DATE) = last_day(CAST(x AS TIMESTAMP))
			THEN CAST(last_day(CAST(x AS TIMESTAMP) + to_months(CAST(n AS INTEGER))) AS TIMESTAMP) + (CAST(x AS TIMESTAMP) - date_trunc('day', CAST(x AS TIMESTAMP)))
			ELSE CAST(x AS TIMESTAMP) + to_months(CAST(n AS INTEGER)) END`,
	// MONTHS_BETWEEN: whole months when both days are the same day of the
	// month or both the last, else months and 31sts of a month.
	`CREATE OR REPLACE TEMP MACRO sf_months_between(a, b) AS CAST(
		(year(CAST(a AS TIMESTAMP)) - year(CAST(b AS TIMESTAMP))) * 12 + month(CAST(a AS TIMESTAMP)) - month(CAST(b AS TIMESTAMP))
		+ CASE WHEN day(CAST(a AS TIMESTAMP)) = day(CAST(b AS TIMESTAMP))
			OR CAST(CAST(a AS TIMESTAMP) AS DATE) = last_day(CAST(a AS TIMESTAMP)) AND CAST(CAST(b AS TIMESTAMP) AS DATE) = last_day(CAST(b AS TIMESTAMP))
			THEN 0 ELSE (day(CAST(a AS TIMESTAMP)) - day(CAST(b AS TIMESTAMP))) / 31.0 END AS DECIMAL(38,6))`,
	// NEXT_DAY and PREVIOUS_DAY, after or before x, never on it.
	`CREATE OR REPLACE TEMP MACRO sf_dow(d) AS CASE upper(left(trim(d), 2))
		WHEN 'MO' THEN 1 WHEN 'TU' THEN 2 WHEN 'WE' THEN 3 WHEN 'TH' THEN 4 WHEN 'FR' THEN 5 WHEN 'SA' THEN 6 WHEN 'SU' THEN 7
		ELSE error('invalid day of the week: ' || d) END`,
	`CREATE OR REPLACE TEMP MACRO sf_next_day(x, d) AS CAST(x AS DATE) + CAST((sf_dow(d) - isodow(x) + 6) % 7 + 1 AS INTEGER)`,
	`CREATE OR REPLACE TEMP MACRO sf_previous_day(x, d) AS CAST(x AS DATE) - CAST((isodow(x) - sf_dow(d) + 6) % 7 + 1 AS INTEGER)`,
	// TO_DATE without a format: the formats of Snowflake's AUTO.
	`CREATE OR REPLACE TEMP MACRO sf_try_to_date(x VARCHAR) AS coalesce(TRY_CAST(x AS DATE), CAST(TRY_CAST(x AS TIMESTAMP) AS DATE),
			CAST(try_strptime(x, '%m/%d/%Y') AS DATE), CAST(try_strptime(x, '%d-%b-%Y') AS DATE)),
		(x DATE) AS x, (x TIMESTAMP) AS CAST(x AS DATE), (x TIMESTAMPTZ) AS CAST(x AS DATE)`,
	`CREATE OR REPLACE TEMP MACRO sf_to_date(x VARCHAR) AS CASE WHEN x IS NULL THEN NULL WHEN sf_try_to_date(x) IS NULL
			THEN error('Date ''' || x || ''' is not recognized') ELSE sf_try_to_date(x) END,
		(x DATE) AS x, (x TIMESTAMP) AS CAST(x AS DATE), (x TIMESTAMPTZ) AS CAST(x AS DATE)`,
	// TO_TIMESTAMP: a number counts seconds since 1970; a string of digits
	// seconds, milliseconds, microseconds or nanoseconds, by its size.
	`CREATE OR REPLACE TEMP MACRO sf_epoch(n) AS CASE
		WHEN abs(n) < 31536000000 THEN make_timestamp(CAST(n * 1000000 AS BIGINT))
		WHEN abs(n) < 31536000000000 THEN make_timestamp(CAST(n * 1000 AS BIGINT))
		WHEN abs(n) < 31536000000000000 THEN make_timestamp(CAST(n AS BIGINT))
		ELSE make_timestamp(CAST(n // 1000 AS BIGINT)) END`,
	`CREATE OR REPLACE TEMP MACRO sf_to_timestamp(x VARCHAR) AS CASE WHEN regexp_full_match(trim(x), '-?[0-9]+')
			THEN sf_epoch(CAST(trim(x) AS HUGEINT)) ELSE CAST(x AS TIMESTAMP) END,
		(x BIGINT) AS make_timestamp(x * 1000000),
		(x DOUBLE) AS make_timestamp(CAST(x * 1000000 AS BIGINT)),
		(x DATE) AS CAST(x AS TIMESTAMP), (x TIMESTAMP) AS x, (x TIMESTAMPTZ) AS CAST(x AS TIMESTAMP)`,
	`CREATE OR REPLACE TEMP MACRO sf_to_timestamp_tz(x VARCHAR) AS CASE WHEN regexp_full_match(trim(x), '-?[0-9]+')
			THEN timezone('UTC', sf_epoch(CAST(trim(x) AS HUGEINT)))
			ELSE CAST(regexp_replace(trim(x), '\s+([+-][0-9]{1,2}(:?[0-9]{2})?)$', '\1') AS TIMESTAMPTZ) END,
		(x BIGINT) AS to_timestamp(x), (x DOUBLE) AS to_timestamp(x),
		(x DATE) AS CAST(x AS TIMESTAMPTZ), (x TIMESTAMP) AS CAST(x AS TIMESTAMPTZ), (x TIMESTAMPTZ) AS x`,
	// TO_BOOLEAN of text: Snowflake's words for true and false; of a
	// number: whether it is not zero.
	`CREATE OR REPLACE TEMP MACRO sf_try_to_boolean(x VARCHAR) AS CASE lower(trim(x))
			WHEN 'true' THEN true WHEN 't' THEN true WHEN 'yes' THEN true WHEN 'y' THEN true WHEN 'on' THEN true WHEN '1' THEN true
			WHEN 'false' THEN false WHEN 'f' THEN false WHEN 'no' THEN false WHEN 'n' THEN false WHEN 'off' THEN false WHEN '0' THEN false END,
		(x BOOLEAN) AS x, (x DOUBLE) AS x <> 0`,
	`CREATE OR REPLACE TEMP MACRO sf_to_boolean(x VARCHAR) AS CASE WHEN x IS NULL THEN NULL WHEN sf_try_to_boolean(x) IS NULL
			THEN error('Boolean value ''' || x || ''' is not recognized') ELSE sf_try_to_boolean(x) END,
		(x BOOLEAN) AS x, (x DOUBLE) AS x <> 0`,
	// CHARINDEX with a start position.
	`CREATE OR REPLACE TEMP MACRO sf_charindex(sub, s, start) AS
		CASE WHEN strpos(substring(s, CAST(start AS BIGINT)), sub) = 0 THEN 0
		ELSE strpos(substring(s, CAST(start AS BIGINT)), sub) + CAST(start AS BIGINT) - 1 END`,
	// STRTOK's tokens: the text between any of the delimiter characters,
	// empty ones skipped.
	`CREATE OR REPLACE TEMP MACRO sf_tokens(s, delims) AS CASE WHEN delims = '' THEN [s] ELSE
		list_filter(string_split(translate(s, delims, repeat(left(delims, 1), length(delims))), left(delims, 1)), lambda t: t <> '') END`,
	// INITCAP: upper case after a delimiter, lower case elsewhere.
	`CREATE OR REPLACE TEMP MACRO sf_initcap(s, d) AS array_to_string(list_transform(range(1, length(s) + 1),
		lambda i: CASE WHEN i = 1 OR strpos(d, substring(s, i - 1, 1)) > 0 THEN upper(substring(s, i, 1)) ELSE lower(substring(s, i, 1)) END), '')`,
	// REGEXP_INSTR: the position of the occ-th match from pos, or after it
	// with opt 1; 0 when there is none.
	`CREATE OR REPLACE TEMP MACRO sf_regexp_instr(s, p, pos, occ, opt) AS
		CASE WHEN len(regexp_extract_all(substring(s, pos), p)) < occ THEN 0
		ELSE pos + length(array_to_string(list_slice(regexp_split_to_array(substring(s, pos), p), 1, occ), ''))
			+ length(array_to_string(list_slice(regexp_extract_all(substring(s, pos), p), 1, occ - 1), ''))
			+ CASE WHEN opt = 1 THEN length(regexp_extract_all(substring(s, pos), p)[occ]) ELSE 0 END END`,
	// SEARCH's tokens: runs of letters and digits, in lower case.
	`CREATE OR REPLACE TEMP MACRO sf_search_tokens(s) AS list_filter(regexp_split_to_array(lower(s), '[^\p{L}\p{N}]+'), lambda t: t <> '')`,
}

// Macros returns the statements that define, in a session, the macros
// translated statements call.
func Macros() []string { return append([]string(nil), macros...) }
