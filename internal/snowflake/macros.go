package snowflake

import "strings"

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
	// VECTOR_COSINE_SIMILARITY, VECTOR_INNER_PRODUCT, VECTOR_L1_DISTANCE
	// and VECTOR_L2_DISTANCE of lists of DOUBLE, which must be as long as
	// each other.
	`CREATE OR REPLACE TEMP MACRO sf_vector_dims(a, b) AS CASE WHEN len(a) <> len(b)
		THEN error('vectors of different dimensions: ' || len(a) || ' and ' || len(b)) END`,
	`CREATE OR REPLACE TEMP MACRO sf_vector_cosine_similarity(a, b) AS coalesce(sf_vector_dims(a, b), list_cosine_similarity(a, b))`,
	`CREATE OR REPLACE TEMP MACRO sf_vector_inner_product(a, b) AS coalesce(sf_vector_dims(a, b), list_inner_product(a, b))`,
	`CREATE OR REPLACE TEMP MACRO sf_vector_l2_distance(a, b) AS coalesce(sf_vector_dims(a, b), list_distance(a, b))`,
	`CREATE OR REPLACE TEMP MACRO sf_vector_l1_distance(a, b) AS
		coalesce(sf_vector_dims(a, b), list_sum(list_transform(range(1, len(a) + 1), lambda i: abs(a[i] - b[i]))))`,

	// Semi-structured values. A VARIANT, ARRAY or OBJECT value is JSON, as
	// are the columns of views and of connections' tables that hold one;
	// a VARCHAR holding JSON text, as tables loaded from text hold them,
	// reads as that JSON. Igloo's ARRAY and OBJECT columns are a list of
	// strings (VARCHAR[]) and a map of strings (MAP(VARCHAR, VARCHAR)),
	// which Iceberg tables hold and other engines read: each element or
	// value as Snowflake's TO_VARCHAR writes it, a string as itself and
	// anything else as its JSON text. These macros read each kind. Those
	// that choose by typeof(x) choose when DuckDB binds the call, since
	// typeof is a constant, so a view keeps working, and is as fast, when
	// its table's column changes from JSON text to a list.
	//
	// sf_elem reads an element or value of such a list or map: the JSON
	// text of an object or array as that object or array, so that objects
	// in an ARRAY keep their paths, anything else as a string, and NULL
	// as JSON null.
	`CREATE OR REPLACE TEMP MACRO sf_elem(e) AS CASE WHEN e IS NULL THEN CAST('null' AS JSON)
		WHEN (starts_with(e, '{') OR starts_with(e, '[')) AND json_valid(e) THEN CAST(e AS JSON) ELSE to_json(e) END`,
	// sf_elem_string reads it as text, as json_extract_string reads what
	// sf_elem makes of it: an object's or an array's JSON text as DuckDB
	// writes it, anything else as itself, without making JSON of a string.
	`CREATE OR REPLACE TEMP MACRO sf_elem_string(e) AS
		CASE WHEN (starts_with(e, '{') OR starts_with(e, '[')) AND json_valid(e) THEN json_extract_string(e, '$') ELSE e END`,
	// A list or a map of strings as JSON, read by sf_elem; when no element
	// is an object's or an array's text, to_json alone, three times faster.
	`CREATE OR REPLACE TEMP MACRO sf_strings_json(x) AS
		CASE WHEN list_bool_or(list_transform(x, lambda e: starts_with(e, '{') OR starts_with(e, '[')))
		THEN to_json(list_transform(x, lambda e: sf_elem(e))) ELSE to_json(x) END`,
	`CREATE OR REPLACE TEMP MACRO sf_map_json(x) AS
		CASE WHEN list_bool_or(list_transform(map_values(x), lambda e: starts_with(e, '{') OR starts_with(e, '[')))
		THEN to_json(map(map_keys(x), list_transform(map_values(x), lambda e: sf_elem(e)))) ELSE to_json(x) END`,
	// sf_json is any value as JSON: a list or map of strings as above, text
	// as the JSON it holds.
	`CREATE OR REPLACE TEMP MACRO sf_json(x) AS CASE
		WHEN typeof(x) = 'VARCHAR[]' THEN sf_strings_json(CAST(x AS VARCHAR[]))
		WHEN typeof(x) = 'MAP(VARCHAR, VARCHAR)' THEN sf_map_json(CAST(x AS MAP(VARCHAR, VARCHAR)))
		ELSE CAST(x AS JSON) END`,
	// sf_to_json is a column passed to a function that builds JSON (an
	// OBJECT_CONSTRUCT's value, an ARRAY_CONSTRUCT's element), as JSON the
	// way those functions take it: a list or map of strings as above, text
	// as a string, JSON as itself.
	`CREATE OR REPLACE TEMP MACRO sf_to_json(x) AS CASE
		WHEN typeof(x) = 'VARCHAR[]' THEN sf_strings_json(CAST(x AS VARCHAR[]))
		WHEN typeof(x) = 'MAP(VARCHAR, VARCHAR)' THEN sf_map_json(CAST(x AS MAP(VARCHAR, VARCHAR)))
		WHEN typeof(x) = 'VARIANT' THEN CAST(x AS JSON)
		ELSE to_json(x) END`,
	// sf_get and sf_get_string read path p of x, as json_extract and
	// json_extract_string do.
	`CREATE OR REPLACE TEMP MACRO sf_get(x, p) AS CASE
		WHEN typeof(x) IN ('VARCHAR[]', 'MAP(VARCHAR, VARCHAR)', 'VARIANT') THEN json_extract(sf_json(x), p)
		ELSE json_extract(x, p) END`,
	`CREATE OR REPLACE TEMP MACRO sf_get_string(x, p) AS CASE
		WHEN typeof(x) IN ('VARCHAR[]', 'MAP(VARCHAR, VARCHAR)', 'VARIANT') THEN json_extract_string(sf_json(x), p)
		ELSE json_extract_string(x, p) END`,
	// FLATTEN's values, keys and indexes of x, with MODE m: lists that
	// FLATTEN unnests side by side (see flattenUnnest). A list of strings
	// and a map of strings give theirs as they are; text, JSON and any
	// other type those of the JSON they are or hold.
	`CREATE OR REPLACE TEMP MACRO sf_flatten_values(x, m) AS CASE
		WHEN typeof(x) IN ('VARCHAR', 'JSON') THEN CASE json_type(CAST(x AS VARCHAR))
			WHEN 'ARRAY' THEN CASE WHEN m <> 'OBJECT' THEN json_extract(CAST(x AS VARCHAR), '$[*]') END
			WHEN 'OBJECT' THEN CASE WHEN m <> 'ARRAY' THEN json_extract(CAST(x AS VARCHAR), '$.*') END END
		WHEN typeof(x) = 'VARCHAR[]' THEN CASE WHEN m <> 'OBJECT' THEN list_transform(CAST(x AS VARCHAR[]), lambda e: sf_elem(e)) END
		WHEN typeof(x) = 'MAP(VARCHAR, VARCHAR)' THEN
			CASE WHEN m <> 'ARRAY' THEN list_transform(map_values(CAST(x AS MAP(VARCHAR, VARCHAR))), lambda e: sf_elem(e)) END
		ELSE CASE json_type(sf_json(x))
			WHEN 'ARRAY' THEN CASE WHEN m <> 'OBJECT' THEN json_extract(sf_json(x), '$[*]') END
			WHEN 'OBJECT' THEN CASE WHEN m <> 'ARRAY' THEN json_extract(sf_json(x), '$.*') END END END`,
	`CREATE OR REPLACE TEMP MACRO sf_flatten_keys(x, m) AS CASE
		WHEN m = 'ARRAY' OR typeof(x) = 'VARCHAR[]' THEN CAST(NULL AS VARCHAR[])
		WHEN typeof(x) IN ('VARCHAR', 'JSON') THEN CASE WHEN json_type(CAST(x AS VARCHAR)) = 'OBJECT' THEN json_keys(CAST(x AS VARCHAR)) END
		WHEN typeof(x) = 'MAP(VARCHAR, VARCHAR)' THEN map_keys(CAST(x AS MAP(VARCHAR, VARCHAR)))
		ELSE CASE WHEN json_type(sf_json(x)) = 'OBJECT' THEN json_keys(sf_json(x)) END END`,
	// THIS, x as JSON, once for each of FLATTEN's rows: DuckDB 1.5 cannot
	// plan an unnest of a lateral subquery next to another expression of
	// the rows it is joined to.
	`CREATE OR REPLACE TEMP MACRO sf_flatten_this(x, m) AS repeat([sf_json(x)], len(sf_flatten_values(x, m)))`,
	`CREATE OR REPLACE TEMP MACRO sf_flatten_indexes(x, m) AS CASE
		WHEN m = 'OBJECT' OR typeof(x) = 'MAP(VARCHAR, VARCHAR)' THEN CAST(NULL AS BIGINT[])
		WHEN typeof(x) IN ('VARCHAR', 'JSON') THEN
			CASE WHEN json_type(CAST(x AS VARCHAR)) = 'ARRAY' THEN range(CAST(json_array_length(CAST(x AS VARCHAR)) AS BIGINT)) END
		WHEN typeof(x) = 'VARCHAR[]' THEN range(CAST(len(CAST(x AS VARCHAR[])) AS BIGINT))
		ELSE CASE WHEN json_type(sf_json(x)) = 'ARRAY' THEN range(CAST(json_array_length(sf_json(x)) AS BIGINT)) END END`,
	// FLATTEN of objects as structs (see flattenStructs) holds the value of
	// a key in two parts: a list of strings when it is one, as Igloo's
	// ARRAY columns are, and JSON otherwise, as OBJECT_CONSTRUCT takes it
	// (see sf_to_json). Both are NULL when an object lacks the key, which
	// OBJECT_CONSTRUCT leaves out when its value is NULL, where
	// OBJECT_CONSTRUCT_KEEP_NULL keeps JSON null.
	`CREATE OR REPLACE TEMP MACRO sf_list_part(x) AS CASE WHEN typeof(x) = 'VARCHAR[]' THEN CAST(x AS VARCHAR[]) END`,
	`CREATE OR REPLACE TEMP MACRO sf_json_part(x) AS CASE WHEN typeof(x) <> 'VARCHAR[]' THEN sf_to_json(x) END`,
	`CREATE OR REPLACE TEMP MACRO sf_json_part_keep_null(x) AS CASE
		WHEN typeof(x) <> 'VARCHAR[]' THEN coalesce(sf_to_json(x), CAST('null' AS JSON))
		WHEN x IS NULL THEN CAST('null' AS JSON) END`,
	// The value of list part l and JSON part j as JSON, and FLATTEN's
	// lists of it with MODE m, as the sf_flatten_ macros give them.
	`CREATE OR REPLACE TEMP MACRO sf_parts_json(l, j) AS CASE WHEN l IS NULL THEN j ELSE sf_strings_json(l) END`,
	`CREATE OR REPLACE TEMP MACRO sf_parts_values(l, j, m) AS CASE WHEN l IS NULL THEN sf_flatten_values(j, m)
		WHEN m <> 'OBJECT' THEN list_transform(l, lambda e: sf_elem(e)) END`,
	// sf_parts_strings is sf_parts_values as text, as json_extract_string
	// reads each value, for a FLATTEN whose values are read only as text.
	`CREATE OR REPLACE TEMP MACRO sf_parts_strings(l, j, m) AS CASE
		WHEN l IS NULL THEN list_transform(sf_flatten_values(j, m), lambda x: json_extract_string(x, '$'))
		WHEN m <> 'OBJECT' THEN list_transform(l, lambda e: sf_elem_string(e)) END`,
	// sf_flatten_strings is sf_flatten_values as text, as json_extract_string
	// reads each value, for a FLATTEN whose values are read only as text.
	`CREATE OR REPLACE TEMP MACRO sf_flatten_strings(x, m) AS CASE
		WHEN typeof(x) = 'VARCHAR[]' THEN CASE WHEN m <> 'OBJECT' THEN list_transform(CAST(x AS VARCHAR[]), lambda e: sf_elem_string(e)) END
		ELSE list_transform(sf_flatten_values(x, m), lambda v: json_extract_string(v, '$')) END`,
	// sf_text_read is text that a select list unnested or built, x, as it
	// is, read in a WHERE, where DuckDB keeps a filter on it above that list
	// (a select list or GROUP BY reads it plainly, as cheaper): it does not
	// push an expression that may raise an error through a projection, and
	// a filter pushed onto an unnest counts as filtering that relation, which
	// DuckDB then estimates at a fifth of its input's rows, where it reckons
	// an unnest gives as many rows as it reads. A join would then build its
	// hash table on the unnest's rows, often far more than the other side's.
	`CREATE OR REPLACE TEMP MACRO sf_text_read(x) AS CASE WHEN strlen(x) < 0 THEN error('') ELSE x END`,
	`CREATE OR REPLACE TEMP MACRO sf_parts_keys(l, j, m) AS CASE WHEN l IS NULL THEN sf_flatten_keys(j, m) END`,
	`CREATE OR REPLACE TEMP MACRO sf_parts_indexes(l, j, m) AS CASE WHEN l IS NULL THEN sf_flatten_indexes(j, m)
		WHEN m <> 'OBJECT' THEN range(CAST(len(l) AS BIGINT)) END`,
	`CREATE OR REPLACE TEMP MACRO sf_parts_this(l, j, m) AS repeat([sf_parts_json(l, j)], len(sf_parts_values(l, j, m)))`,
	// A key's value as three parts: a list of strings l as above, a string s,
	// which a VARCHAR is in OBJECT_CONSTRUCT, and JSON j otherwise, of a
	// column as sf_to_json takes it (sf_other_part) or of any other value
	// as to_json does (sf_expr_part). sf_fields_json and sf_fields_string
	// read the three as JSON and as text.
	`CREATE OR REPLACE TEMP MACRO sf_string_part(x) AS CASE WHEN typeof(x) = 'VARCHAR' THEN CAST(x AS VARCHAR) END`,
	`CREATE OR REPLACE TEMP MACRO sf_other_part(x) AS CASE WHEN typeof(x) NOT IN ('VARCHAR[]', 'VARCHAR') THEN sf_to_json(x) END`,
	`CREATE OR REPLACE TEMP MACRO sf_other_part_keep_null(x) AS CASE
		WHEN typeof(x) NOT IN ('VARCHAR[]', 'VARCHAR') THEN coalesce(sf_to_json(x), CAST('null' AS JSON))
		WHEN x IS NULL THEN CAST('null' AS JSON) END`,
	`CREATE OR REPLACE TEMP MACRO sf_expr_part(x) AS CASE WHEN typeof(x) <> 'VARCHAR' THEN to_json(x) END`,
	`CREATE OR REPLACE TEMP MACRO sf_expr_part_keep_null(x) AS CASE
		WHEN typeof(x) <> 'VARCHAR' THEN coalesce(to_json(x), CAST('null' AS JSON))
		WHEN x IS NULL THEN CAST('null' AS JSON) END`,
	`CREATE OR REPLACE TEMP MACRO sf_fields_json(l, s, j) AS CASE WHEN s IS NOT NULL THEN to_json(s) ELSE sf_parts_json(l, j) END`,
	`CREATE OR REPLACE TEMP MACRO sf_fields_string(l, s, j) AS coalesce(s, json_extract_string(sf_parts_json(l, j), '$'))`,
	// Division (see divisions). Snowflake divides two NUMBERs into a NUMBER
	// whose scale is the dividend's plus 6, at least 6 and at most 12 (or
	// the dividend's own, if larger), rounded half away from zero: 1/3 is
	// 0.333333, 41/640 is 0.064063. A FLOAT on either side gives a FLOAT.
	// Dividing anything but NULL by zero fails, FLOATs too. DuckDB's typeof
	// is a constant for any argument, aggregates and windows included, so
	// the scale and the branch are chosen when the statement is bound; the
	// result is a DOUBLE, as DuckDB's / gives. sf_num_scale is the scale of
	// a NUMBER of DuckDB type t (0 for an integer), NULL for any other type.
	`CREATE OR REPLACE TEMP MACRO sf_num_scale(t) AS CASE
		WHEN t IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER') THEN 0
		WHEN t LIKE 'DECIMAL(%' THEN CAST(regexp_extract(t, ',(\d+)\)', 1) AS INTEGER) END`,
	// Integers are divided exactly, in BIGINT while it holds the operands
	// (about 1 ns a row more than DuckDB's /), else in HUGEINT; q is
	// DuckDB's quotient, for operands of more than 30 digits.
	`CREATE OR REPLACE TEMP MACRO sf_div_int(a, b, q) AS CASE
		WHEN abs(a) < 4000000000000 AND abs(b) < 1000000000000000000 THEN CAST(sign(a) * sign(b) *
			((2 * abs(CAST(a AS BIGINT)) * 1000000 + abs(CAST(b AS BIGINT))) // (2 * abs(CAST(b AS BIGINT)))) AS DOUBLE) / 1000000
		WHEN abs(a) < 1000000000000000000000000000000 AND abs(b) < 10000000000000000000000000000000000000 THEN CAST(sign(a) * sign(b) *
			((2 * abs(CAST(a AS HUGEINT)) * 1000000 + abs(CAST(b AS HUGEINT))) // (2 * abs(CAST(b AS HUGEINT)))) AS DOUBLE) / 1000000
		ELSE round(q * (1 + 1e-15), 6) END`,
	// A decimal side: the DOUBLE quotient, moved away from zero by more
	// than its own error (a few units in the last place) and rounded, which
	// is exact while the quotient's digits, scale included, number fewer
	// than about 14.
	`CREATE OR REPLACE TEMP MACRO sf_div_scaled(a, b, s1, s2) AS CASE
		WHEN b = 0 AND a IS NOT NULL THEN error('Division by zero')
		WHEN s1 IS NULL OR s2 IS NULL THEN a / b
		WHEN s1 = 0 AND s2 = 0 THEN sf_div_int(a, b, a / b)
		ELSE round(CAST(a AS DOUBLE) / CAST(b AS DOUBLE) * (1 + 1e-15), greatest(s1, least(s1 + 6, 12))) END`,
	`CREATE OR REPLACE TEMP MACRO sf_div(a, b) AS sf_div_scaled(a, b, sf_num_scale(typeof(a)), sf_num_scale(typeof(b)))`,
	// sf_div names each argument several times, and DuckDB computes a
	// subquery or a volatile function as many times as it is named; this
	// computes each once.
	`CREATE OR REPLACE TEMP MACRO sf_div_once(a, b) AS list_transform([{'a': a, 'b': b}], lambda s: sf_div(s.a, s.b))[1]`,
	// Snowflake's implicit conversions, which translations with
	// Options.Implicit call. sf_str is any value as text, as a string
	// function reads it: text as itself, which costs nothing, anything else
	// as TO_VARCHAR writes it. sf_as is a, compared with b: text cast to
	// b's type when b is a date, a time or a number, as Snowflake casts it,
	// and a as it is otherwise.
	`CREATE OR REPLACE TEMP MACRO sf_str(x VARCHAR) AS x, (x) AS ` + strings.ReplaceAll(defaultText, "{x}", "x"),
	`CREATE OR REPLACE TEMP MACRO sf_as(a VARCHAR, b TIMESTAMP) AS CAST(a AS TIMESTAMP),
		(a VARCHAR, b TIMESTAMPTZ) AS CAST(a AS TIMESTAMPTZ), (a VARCHAR, b DATE) AS CAST(a AS DATE),
		(a VARCHAR, b TIME) AS CAST(a AS TIME), (a VARCHAR, b DOUBLE) AS CAST(a AS DOUBLE), (a, b) AS a`,
}

// Macros returns the statements that define, in a session, the macros
// translated statements call.
func Macros() []string { return append([]string(nil), macros...) }
