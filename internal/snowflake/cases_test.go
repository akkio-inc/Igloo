package snowflake

// snowflakeCases are Snowflake statements with the results Snowflake
// returned for them (Snowflake, October 2026, session time zone UTC where
// it matters). Values are written as format writes DuckDB's: timestamps
// without a T, zones as Z, JSON compact. Where Igloo's result differs on
// purpose, the comment says how; docs/snowflake-sql.md lists every
// difference.
var snowflakeCases = []runCase{
	// DATEADD keeps a DATE a DATE for parts of a day or more.
	{name: "dateadd date", sql: `select dateadd(day, 1, '2024-01-01'::date)`, want: []string{"2024-01-02"}, types: []string{"DATE"}},
	{name: "dateadd string", sql: `select dateadd(day, 1, '2024-01-01')`, want: []string{"2024-01-02 00:00:00"}, types: []string{"TIMESTAMP"}},
	{name: "dateadd hour of date", sql: `select dateadd(hour, 1, '2024-01-01'::date)`, want: []string{"2024-01-01 01:00:00"}, types: []string{"TIMESTAMP"}},
	{name: "dateadd rounds", sql: `select dateadd(day, 1.6, '2024-01-01'::date), dateadd(day, -1.5, '2024-01-10'::date)`,
		want: []string{"2024-01-03|2024-01-08"}},
	{name: "dateadd month end", sql: `select dateadd(month, 1, '2024-01-31'::date), dateadd(year, 1, '2024-02-29'::date), dateadd(quarter, 1, '2024-01-31'::date)`,
		want: []string{"2024-02-29|2025-02-28|2024-04-30"}},
	{name: "timestampadd", sql: `select timestampadd(minute, 90, '2024-01-01 10:00'::timestamp), dateadd('week', 2, '2024-01-01'::date)`,
		want: []string{"2024-01-01 11:30:00|2024-01-15"}},
	{name: "dateadd small parts", sql: `select dateadd(ms, 1500, '2024-01-01 10:00'::timestamp), dateadd(ns, 1000, '2024-01-01 10:00'::timestamp), dateadd(minute, 30, '10:00'::time), dateadd(month, 1, '2024-01-31 10:00'::timestamp)`,
		want: []string{"2024-01-01 10:00:01.5|2024-01-01 10:00:00.000001|10:30:00|2024-02-29 10:00:00"}},

	// DuckDB cannot choose a typed macro's overload for an aggregate, which
	// the translation takes inside MIN, MAX and the like.
	{name: "dates of aggregates", sql: `select dateadd(day, 1, max(d)), date_trunc('month', min(d)), add_months(max(d), 1), to_date(max(s)), dateadd(day, count(*), '2024-01-01'::date) from (values ('2024-01-31'::date, '2024-02-01')) t(d, s)`,
		want: []string{"2024-02-01|2024-01-01|2024-02-29|2024-02-01|2024-01-02"}, types: []string{"DATE", "DATE", "DATE", "DATE", "DATE"}},
	{name: "dates of windows", sql: `select date_trunc('month', lag(d) over (order by d)) from (values ('2024-01-31'::date), ('2024-03-05'::date)) t(d) order by 1 nulls first`,
		want: []string{"NULL", "2024-01-01"}, types: []string{"DATE"}},

	// DATE_TRUNC and TRUNC keep the input's type.
	{name: "date_trunc", sql: `select date_trunc('hour', '2024-01-01'::date), date_trunc('week', '2024-03-14'::date), date_trunc('WEEK', '2024-03-14 10:11:12'::timestamp), date_trunc(quarter, '2024-05-14'::date), trunc('2024-05-14'::date, 'MM'), date_trunc('minute', '10:11:12'::time)`,
		want: []string{"2024-01-01|2024-03-11|2024-03-11 00:00:00|2024-04-01|2024-05-01|10:11:00"}, types: []string{"DATE", "DATE", "TIMESTAMP", "DATE", "DATE", "TIME"}},
	{name: "trunc numbers", sql: `select trunc(1.567, 2), trunc(-1.567), truncate(1.567, 1), trunc(1567, -2)`, want: []string{"1.56|-1|1.5|1500"}},

	// TIME_SLICE counts slices from 1970-01-01 (weeks from Monday).
	{name: "time_slice", sql: `select time_slice('2024-03-14'::date, 1, 'WEEK'), time_slice('2024-03-14 10:17:00'::timestamp, 15, 'MINUTE'), time_slice('2024-03-14'::date, 1, 'MONTH'), time_slice('2024-03-14'::date, 2, 'DAY'), time_slice('2024-03-14 10:17:00'::timestamp, 15, 'MINUTE', 'END'), time_slice('2024-03-14'::date, 3, 'MONTH'), time_slice('2024-03-14'::date, 2, 'WEEK')`,
		want: []string{"2024-03-11|2024-03-14 10:15:00|2024-03-01|2024-03-14|2024-03-14 10:30:00|2024-01-01|2024-03-11"}},

	// DATEDIFF counts boundaries; weeks start on Monday.
	{name: "datediff", sql: `select datediff(week, '2024-03-09', '2024-03-11'), datediff(week, '2024-03-10', '2024-03-11'), datediff(month, '2024-01-31', '2024-02-01'), datediff(year, '2023-12-31', '2024-01-01'), datediff(second, '2024-01-01 10:00:00.9'::timestamp, '2024-01-01 10:00:01.1'::timestamp), datediff(day, '2024-01-01 23:59'::timestamp, '2024-01-02 00:01'::timestamp), datediff('day', '2024-01-05'::date, '2024-01-01'::date), datediff(quarter, '2024-03-31', '2024-04-01'), datediff(week, '2024-03-11', '2024-03-17'), datediff(week, '2024-03-17', '2024-03-18'), timestampdiff(hour, '2024-01-01 10:59'::timestamp, '2024-01-01 11:01'::timestamp), datediff(week, '2024-03-18', '2024-03-10'), datediff(minute, '2024-01-01 10:00:59'::timestamp, '2024-01-01 10:01:00'::timestamp), datediff(millisecond, '2024-01-01 10:00:00.0019'::timestamp, '2024-01-01 10:00:00.0021'::timestamp)`,
		want: []string{"1|1|1|1|1|1|-4|1|0|1|1|-2|1|1"}},

	// Date parts: Sunday is day 0, weeks are ISO weeks.
	{name: "date parts", sql: `select dayofweek('2024-03-10'::date), dayofweekiso('2024-03-10'::date), week('2024-01-01'::date), weekofyear('2021-01-01'::date), yearofweek('2021-01-01'::date), yearofweekiso('2021-01-01'::date), weekiso('2021-01-01'::date), dayofyear('2024-03-10'::date), quarter('2024-05-01'::date), date_part(epoch_second, '2024-01-01 00:00:01.5'::timestamp), date_part(epoch_millisecond, '2024-01-01 00:00:01.5'::timestamp), extract(dow from '2024-03-10'::date), date_part('nanosecond', '2024-01-01 00:00:01.5'::timestamp), extract(dayofweek_iso from '2024-03-10'::date), date_part(year, '2024-03-10'::date), second('2024-01-01 00:00:01.5'::timestamp), extract(epoch from '2024-01-01 00:00:01.5'::timestamp)`,
		want: []string{"0|7|1|53|2020|2020|53|70|2|1704067201|1704067201500|0|500000000|7|2024|1|1704067201"}},
	{name: "day names", sql: `select dayname('2024-03-10'::date), monthname('2024-03-10'::date), last_day('2024-02-10'::date), last_day('2024-03-14'::date, 'week'), last_day('2024-03-14'::date, 'year'), last_day('2024-03-14'::date, 'quarter'), next_day('2024-03-14'::date, 'Friday'), next_day('2024-03-14'::date, 'th'), previous_day('2024-03-14'::date, 'Thursday'), last_day('2024-02-10 10:00'::timestamp)`,
		want: []string{"Sun|Mar|2024-02-29|2024-03-17|2024-12-31|2024-03-31|2024-03-15|2024-03-21|2024-03-07|2024-02-29"}},
	{name: "add_months", sql: `select add_months('2024-02-29'::date, 1), add_months('2024-01-31'::date, 1), months_between('2024-03-15'::date, '2024-02-15'::date), months_between('2024-03-31'::date, '2024-02-29'::date), months_between('2024-03-20'::date, '2024-02-15'::date)`,
		want: []string{"2024-03-31|2024-02-29|1.000000|1.000000|1.161290"}},
	{name: "from parts", sql: `select date_from_parts(2024, 13, 1), date_from_parts(2024, 1, 0), timestamp_from_parts(2024, 1, 1, 10, 11, 12), time_from_parts(10, 11, 12)`,
		want: []string{"2025-01-01|2023-12-31|2024-01-01 10:11:12|10:11:12"}},
	// The two-argument form gives the time in the zone; Snowflake's is a
	// TIMESTAMP_TZ showing the same time with the zone's offset.
	{name: "convert_timezone", sql: `select convert_timezone('UTC', 'America/New_York', '2024-01-01 12:00:00'::timestamp_ntz), convert_timezone('America/New_York', '2024-01-01 12:00:00 +00:00'::timestamp_tz)`,
		want: []string{"2024-01-01 07:00:00|2024-01-01 07:00:00"}},

	// Conversions.
	{name: "to_timestamp numbers", sql: `select to_timestamp(1700000000), to_timestamp(1700000000.5), to_timestamp('1700000000'), to_timestamp('1700000000000'), to_timestamp('1700000000123456')`,
		want: []string{"2023-11-14 22:13:20|2023-11-14 22:13:20.5|2023-11-14 22:13:20|2023-11-14 22:13:20|2023-11-14 22:13:20.123456"}},
	{name: "to_timestamp scale", sql: `select to_timestamp_ltz(1735689600000, 3), to_timestamp_ntz(1700000000123, 3), to_timestamp(1700000000123456, 6)`,
		want: []string{"2025-01-01 00:00:00Z|2023-11-14 22:13:20.123|2023-11-14 22:13:20.123456"}},
	{name: "to_date", sql: `select to_date('2024-03-14'), to_date('03/14/2024', 'MM/DD/YYYY'), to_timestamp('2024-03-14 10:11:12'), to_timestamp('14.03.2024 10:11', 'DD.MM.YYYY HH24:MI'), to_date('2024-03-14 10:11:12'), try_to_date('bad'), to_time('10:11:12'), to_date('20240314', 'YYYYMMDD'), date('2024-03-14'), to_timestamp('2024-03-14T10:11:12Z'), to_date('03/14/2024')`,
		want: []string{"2024-03-14|2024-03-14|2024-03-14 10:11:12|2024-03-14 10:11:00|2024-03-14|NULL|10:11:12|2024-03-14|2024-03-14|2024-03-14 10:11:12|2024-03-14"}},
	{name: "to_date bad", sql: `select to_date('bad')`, wantErr: "is not recognized"},
	{name: "to_timestamp_tz", sql: `select to_timestamp_tz('2024-03-14 10:11:12 +05:00')`, want: []string{"2024-03-14 05:11:12Z"}},
	{name: "to_number", sql: `select to_number('12.7'), to_number('1,234.5', '9,999.9', 10, 1), to_decimal('12.345', 10, 2), to_number(12.5), try_to_number('x'), to_double('1.5'), to_boolean('yes'), to_boolean('off')`,
		want: []string{"13|1234.5|12.35|13|NULL|1.5|true|false"}},
	{name: "casts round", sql: `select 2.5::int, 3.5::int, -0.5::int, '12.345'::number(10,2), 12.345::number(10,2), 1.005::number(10,2), '2.7'::int, 1.5::decimal`,
		want: []string{"3|4|-1|12.35|12.35|1.01|3|2"}},
	{name: "rounding", sql: `select round(2.5), round(-2.5), round(2.5::float), round(1.2345, 2), ceil(1.234, 1), floor(-1.234, 1), 0.1::float, 1e2`,
		want: []string{"3|-3|3|1.23|1.3|-1.3|0.1|100"}},
	{name: "to_char dates", sql: `select to_char('2024-03-04'::date, 'YYYY-MM-DD'), to_char('2024-03-04 15:04:05'::timestamp, 'MON DD, YYYY HH12:MI AM'), to_varchar('2024-03-04 15:04:05.123456'::timestamp), to_char('2024-03-04'::date, 'MM/DD/YYYY'), to_varchar('2024-03-04'::date), to_char('2024-03-04 15:04:05'::timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'), to_char('2024-03-04'::date, 'DY, Mon DD'), to_char('2024-03-04'::date, 'yyyy-mm'), to_char('2024-03-04 15:04:05'::timestamp, 'YYYYMMDDHH24MISS')`,
		want: []string{"2024-03-04|Mar 04, 2024 03:04 PM|2024-03-04 15:04:05.123|03/04/2024|2024-03-04|2024-03-04T15:04:05.000|Mon, Mar 04|2024-03|20240304150405"}},
	{name: "to_char day names", sql: `select to_char('2024-03-10'::date, 'DY'), to_char('2024-03-10'::date, 'MON'), to_char('2024-03-10'::date, 'MMMM'), to_char('2024-03-10 15:00'::timestamp, 'HH12 AM PM'), to_char('2024-03-10 05:00'::timestamp, 'HH12:MI pm')`,
		want: []string{"Sun|Mar|March|03 PM PM|05:00 AM"}},
	{name: "to_char values", sql: `select to_char(3.14159::float), to_varchar(1.50::number(10,2)), to_varchar(true)`, want: []string{"3.14159|1.50|true"}},
	{name: "to_char numbers", sql: `select to_char(1234567.891, '999,999,999.00'), to_char(12, '000'), to_char(0.5, '0.00'), to_char(-1234.5, '9,999.99'), to_char(1234.5, 'FM9,999.99'), to_char(123456, '9,999'), to_char(5, '$99.00'), to_char(12.3, '999.9'), to_char(0.12, '9.99'), to_char(1234.567, 'FM9999.0'), to_char(1234, '999999')`,
		want: []string{"   1,234,567.89| 012| 0.50|-1,234.5 |1,234.5| #,###|  $5.00|  12.3| 0.12|1234.6|   1234"}},

	// Strings.
	{name: "string escapes", sql: `select 'a\'b', 'a\\b', 'a\nb', $$a'b\n$$, 'a''b', 'tab\there', '\x41', 'é'`,
		want: []string{"a'b|a\\b|a\nb|a'b\\n|a'b|tab\there|A|é"}},
	{name: "split_part and substr", sql: `select split_part('a,b,c', ',', 0), split_part('a,b,c', ',', -1), split_part('a,b,c', ',', 5), substr('abcdef', 0, 2), substr('abcdef', -3, 2), substring('abcdef', 2), left('abc', 2), right('abc', 2), strtok('a,,b', ',', 2), strtok('a b c'), strtok('a,,b', ',', 3), len('abc'), charindex('b', 'abc'), charindex('b', 'abcb', 3), position('b', 'abc'), position('b' in 'abc'), charindex('z', 'abc')`,
		want: []string{"a|c||ab|de|bcdef|ab|bc|b|a|NULL|3|2|4|2|2|0"}},
	{name: "substr edges", sql: `select substr('abcdef', 0), substr('abcdef', 1, 0), substring('abcdef', -2), left('abc', 0), left('abc', -1)`,
		want: []string{"abcdef||ef||"}},
	{name: "regexp", sql: `select regexp_substr('abc123def456', '[0-9]+', 1, 2), regexp_substr('abc', '[0-9]+'), regexp_like('abc', 'b'), regexp_like('abc', 'a.c'), 'abc' rlike 'a.c', regexp_replace('aaa', 'a', 'b'), regexp_count('abcabc', 'b'), regexp_substr('ABC', 'b', 1, 1, 'i'), regexp_substr('key=val', '(\\w+)=(\\w+)', 1, 1, 'e', 2), regexp_replace('abc', '(b)', '[\\1]'), regexp_instr('abcabc', 'c', 1, 2), regexp_substr('a1b22', '\\d+', 3), 'abc' regexp 'a.*', regexp_like('ABC', 'abc', 'i')`,
		want: []string{"456|NULL|false|true|true|bbb|2|B|val|a[b]c|6|22|true|true"}},
	{name: "like any", sql: `select 'abc' like any ('a%', 'z%'), 'abc' ilike any ('A%', 'z%'), 'abc' like all ('a%', '%c'), 'a_c' like 'a\\_c', 'abc' like 'a\\_c', 'a%c' like 'a!%c' escape '!'`,
		want: []string{"true|true|true|false|false|true"}},
	{name: "null semantics", sql: `select greatest(1, null), least(1, null), concat('a', null), concat_ws(',', 'a', null), 'a' || null, 1 || 'a', greatest_ignore_nulls(1, null), concat('a', 1, 2.5)`,
		want: []string{"NULL|NULL|NULL|NULL|NULL|1a|1|a12.5"}},
	{name: "concat types", sql: `select concat(1, 2), concat('2024-01-01'::date, 'x'), 'x' || 1.50::number(5,2), concat('a', 'b', 'c')`,
		want: []string{"12|2024-01-01x|x1.50|abc"}},
	// Division of two NUMBERs is a NUMBER of the dividend's scale plus 6
	// (at least 6, at most 12), rounded half away from zero. Igloo's is
	// that value as a DOUBLE: Snowflake's 0.064063 and 3.500000.
	{name: "division", sql: `select 1/3, 41/640, -41/640, 41/-640, -1/2000000, 3/2000000, 1/3*3, 7/2, cast(0.41 as number(10,2))/640, cast(1 as number(38,0))/7, 10/2.5, 1.5/0.7, 2*3/4, 2*(3/4), -3/4*2, cast(1.12345678 as number(20,8))/3, cast(1 as float)/4`,
		want:  []string{"0.333333|0.064063|-0.064063|-0.064063|-0.000001|0.000002|0.999999|3.5|0.00064063|0.142857|4|2.1428571|1.5|1.5|-1.5|0.374485593333|0.25"},
		types: []string{"DOUBLE", "DOUBLE"}},
	// A FLOAT on either side gives a FLOAT, not rounded.
	{name: "division of floats", sql: `select 1::float/3, 1/3::float, cast(1.5 as number(10,1))/7::float, 1::float/3::float`,
		want:  []string{"0.3333333333333333|0.3333333333333333|0.21428571428571427|0.3333333333333333"},
		types: []string{"DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE"}},
	// Dividing by zero fails, FLOATs too, unless the dividend is NULL;
	// DIV0 and DIV0NULL give 0 (Snowflake: 0.000000 and 0E-7).
	{name: "division by zero", sql: `select 1/0`, wantErr: "Division by zero"},
	{name: "float division by zero", sql: `select 1::float/0`, wantErr: "Division by zero"},
	{name: "division of a column by zero", sql: `select x/y from (values (1, 2), (0, 0)) t(x, y)`, wantErr: "Division by zero"},
	{name: "division by zero of null", sql: `select null/0, 1/nullif(0, 0), div0(1, 0), div0(1.5, 0), x/y from (values (1, 0), (1, 2)) t(x, y) where y <> 0`,
		want: []string{"NULL|NULL|0|0|0.5"}},
	{name: "division of aggregates", sql: `select sum(x) / count(*), sum(x) over () / count(*) over (), div0(1, 3), div0null(2, 3), 1 / (select count(*) from (values (1), (2), (3))) from (values (1), (2), (2)) t(x) group by x order by 1`,
		want: []string{"1|1.5|0.333333|0.666667|0.333333", "2|1.5|0.333333|0.666667|0.333333"}},
	{name: "conditionals", sql: `select equal_null(null, null), decode(null, null, 'x', 'y'), div0(1, 0), div0null(1, null), zeroifnull(null), nullifzero(0), nvl2(null, 1, 2), iff(null, 1, 2), square(3)`,
		// Snowflake's DIV0 results are NUMBER(.., 6): 0.000000.
		want: []string{"true|x|0|0|0|NULL|2|2|9"}},
	{name: "string functions", sql: `select editdistance('kitten', 'sitting'), initcap('hello world-foo bar_baz o''neil'), space(3) || '|', lpad('a', 3), replace('abc', 'b'), reverse('abc'), translate('abc', 'ab', 'xy'), md5('a'), sha2('a'), sha1('a'), startswith('abc', 'ab'), contains('abc', 'bc')`,
		want: []string{"3|Hello World-Foo Bar_Baz O'neil|   ||  a|ac|cba|xyc|0cc175b9c0f1b6a831c399e269772661|ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb|86f7e437faa5a7fce15d1ddcb9eaeaea377667b8|true|true"}},
	{name: "search", sql: `select search('two words here', 'words'), search('two words here', 'words here', search_mode => 'PHRASE'), search('two words here', 'nope words', search_mode => 'OR'), search('two words here', 'nope words', search_mode => 'AND'), search('Two-Words', 'two')`,
		want: []string{"true|true|true|false|true"}},

	// Aggregates.
	{name: "listagg and array_agg", sql: `select listagg(v, ',') within group (order by v), listagg(distinct v, '|') within group (order by v), array_agg(v) within group (order by v), array_agg(v) within group (order by v desc), count(*) from (values ('x'),(null),('y'),('x')) t(v)`,
		want: []string{`x,x,y|x|y|["x","x","y"]|["y","x","x"]|4`}},
	{name: "aggregates of nothing", sql: `select listagg(v, ','), array_agg(v), object_agg(v, v::variant), count(*) from (values ('x')) t(v) where 1=0`,
		want: []string{`|[]|{}|0`}},
	{name: "aggregates of nulls", sql: `select listagg(v, ','), array_agg(v) from (values (null::varchar)) t(v)`, want: []string{`|[]`}},
	{name: "listagg window", sql: `select v, listagg(v, ',') within group (order by v) over (partition by g) from (values ('x', 1),('y', 1),('z', 2)) t(v, g) order by v`,
		want: []string{"x|x,y", "y|x,y", "z|z"}},
	{name: "median and mode", sql: `select median(x), mode(x) from (values (1),(2),(3),(4)) t(x)`, want: []string{"2.5|1"}},
	{name: "count distinct", sql: `select count(distinct a, b) from (values (1, 1), (1, null), (null, null), (1, 1), (2, 1)) t(a, b)`, want: []string{"2"}},
	{name: "ratio_to_report", sql: `select x, ratio_to_report(x) over () from (values (1),(2)) t(x) order by x`,
		// Snowflake: 0.333333 and 0.666667 (NUMBER).
		want: []string{"1|0.333333", "2|0.666667"}},
	{name: "object_agg", sql: `select object_agg(k, v) from (values ('a', 1), ('b', null), (null, 2)) t(k, v)`, want: []string{`{"a":1}`}},
	{name: "array_agg of objects", sql: `select array_agg(object_construct('k', k, 'v', v)) within group (order by k) from (values ('a', 1), ('b', null)) t(k, v)`,
		want: []string{`[{"k":"a","v":1},{"k":"b"}]`}},
	{name: "approx_top_k", sql: `select approx_top_k(x, 2) from (values ('a'),('a'),('b'),('c'),('c'),('c')) t(x)`, want: []string{`[["c",3],["a",2]]`}},
	{name: "hll", sql: `select hll(x), approx_count_distinct(x) from (values (1),(2),(2)) t(x)`, want: []string{"2|2"}},
	{name: "grouping", sql: `select a, sum(b), grouping(a) from (values (1, 2), (1, 3), (2, 5)) t(a, b) group by rollup(a) order by a`,
		want: []string{"1|5|0", "2|5|0", "NULL|10|1"}},
	{name: "conditional_true_event", sql: `select x, conditional_true_event(x > 1) over (order by x) from (values (1),(2),(1),(3)) t(x) order by x`,
		want: []string{"1|0", "1|0", "2|1", "3|2"}},
	{name: "lag ignore nulls", sql: `select x, lag(x) ignore nulls over (order by o) from (values (1, 1), (null, 2), (3, 3)) t(x, o) order by o`,
		want: []string{"1|NULL", "NULL|1", "3|1"}},

	// Semi-structured data.
	{name: "arrays", sql: `select array_size(array_construct(1,2)), array_to_string(array_construct('a', null, 'b'), ','), array_contains('b'::variant, array_construct('a','b')), array_slice(array_construct(1,2,3,4), 1, 3), split('a,b,c', ','), split('a,b,c', ',')[0], array_construct('a','b')[1]::string, [1, 2, 3], array_size(parse_json('{"a":1}')), arrays_overlap(array_construct(1,2), array_construct(2,3))`,
		want: []string{`2|a,,b|true|[2,3]|["a","b","c"]|"a"|b|[1,2,3]|NULL|true`}},
	// Snowflake shows ARRAY_CONSTRUCT's NULL as undefined.
	{name: "array_construct null", sql: `select array_construct(1, null, 3), array_to_string(array_construct(1, 2.5, 'x', true), '-')`, want: []string{`[1,null,3]|1-2.5-x-true`}},
	{name: "objects", sql: `select object_construct('a', 1, 'b', null), object_construct_keep_null('a', 1, 'b', null), object_keys(parse_json('{"b":1,"a":2}')), {'x': 1, 'y': 'z'}, object_insert(object_construct('a', 1), 'b', 2), object_construct('b', 1, 'a', 2)`,
		want: []string{`{"a":1}|{"a":1,"b":null}|["a","b"]|{"x":1,"y":"z"}|{"a":1,"b":2}|{"a":2,"b":1}`}},
	{name: "paths", sql: `select v:a.b[1], v:s, v:s::string, v:n, v['a']['b'][0], v:a, v:n > 5, v:s = 'x', v:f > 2, v:s || 'y', upper(v:s), v:nul, v:nul is null, v:missing is null, typeof(v:s), typeof(v:n), v:n + 1, get(v, 's'), get_path(v, 'a.b[0]'), v:"s", v:a:b, coalesce(v:s, 'q'), v:n::number(10,2), v:t::boolean from (select parse_json('{"a":{"b":[1,2]},"s":"x","n":10,"f":2.5,"t":true,"nul":null}') v)`,
		// A JSON null reaches clients as NULL (Snowflake: null).
		want: []string{`2|"x"|x|10|1|{"b":[1,2]}|true|true|true|xy|X|NULL|false|true|VARCHAR|INTEGER|11|"x"|1|"x"|[1,2]|x|10.00|true`}},
	{name: "path arithmetic", sql: `select v:n * 2, v:s = v:s2, v:n = 10, v:n in (10, 11), v:s in ('x', 'y'), v:s like 'x%', v:n between 5 and 15 from (select parse_json('{"n":10,"s":"x","s2":"x"}') v)`,
		want: []string{"20|true|true|true|true|true|true"}},
	{name: "path aggregates", sql: `select sum(v:n), max(v:s), min(v:n) from (select parse_json(column1) v from values ('{"n":10,"s":"b"}'), ('{"n":9,"s":"a"}'))`,
		want: []string{`19|"b"|9`}},
	{name: "path order", sql: `select v:n from (select parse_json(column1) v from values ('{"n":10}'), ('{"n":9}'), ('{"n":"b"}'), ('{"n":"a"}'), ('{}')) order by v:n`,
		want: []string{"9", "10", `"a"`, `"b"`, "NULL"}},
	{name: "json null", sql: `select parse_json('{"a":null}'):a::string, parse_json('{"a":null}'):a is null, to_json(parse_json('{"a":null}'):a)`,
		want: []string{"NULL|false|null"}},
	{name: "variant casts", sql: `select parse_json('"abc"')::string, parse_json('{"k":"v"}')::string, parse_json('5')::string, to_variant('x')::string, parse_json('5')::int, parse_json('"5"')::int, parse_json('null')::string`,
		want: []string{`abc|{"k":"v"}|5|x|5|5|NULL`}},
	{name: "flatten array", sql: `select key, path, index, value, this from table(flatten(input => parse_json('[10, "x", {"k": 1}]')))`,
		want: []string{`NULL|[0]|0|10|[10,"x",{"k":1}]`, `NULL|[1]|1|"x"|[10,"x",{"k":1}]`, `NULL|[2]|2|{"k":1}|[10,"x",{"k":1}]`}},
	{name: "flatten object", sql: `select key, path, index, value from table(flatten(input => parse_json('{"a": 1, "b": [2, 3]}')))`,
		want: []string{`a|a|NULL|1`, `b|b|NULL|[2,3]`}},
	{name: "flatten path", sql: `select key, path, index, value, this from table(flatten(input => parse_json('{"a": {"b": [2, 3]}}'), path => 'a.b'))`,
		want: []string{`NULL|a.b[0]|0|2|[2,3]`, `NULL|a.b[1]|1|3|[2,3]`}},
	{name: "flatten recursive", sql: `select key, path, index, value, this from table(flatten(input => parse_json('{"a": 1, "b": [2, {"c": 3}]}'), recursive => true))`,
		want: []string{`a|a|NULL|1|{"a":1,"b":[2,{"c":3}]}`, `b|b|NULL|[2,{"c":3}]|{"a":1,"b":[2,{"c":3}]}`, `NULL|b[0]|0|2|[2,{"c":3}]`,
			`NULL|b[1]|1|{"c":3}|[2,{"c":3}]`, `c|b[1].c|NULL|3|{"c":3}`}},
	{name: "flatten outer", sql: `select t.id, f.key, f.path, f.index, f.value, f.this from (select 1 id, parse_json('[]') arr union all select 2, parse_json('[5]')) t, lateral flatten(input => t.arr, outer => true) f order by 1`,
		want: []string{`1|NULL||NULL|NULL|[]`, `2|NULL|[0]|0|5|[5]`}},
	{name: "flatten scalar", sql: `select * from table(flatten(input => parse_json('5')))`, want: nil},
	{name: "flatten values", sql: `select f.value, f.value::string, f.value:k, f.index, f.key, sum(f.value) over () from (select parse_json('[1, 2]') arr), lateral flatten(arr) f order by f.index`,
		want: []string{`1|1|NULL|0|NULL|3`, `2|2|NULL|1|NULL|3`}},
	{name: "flatten value compared", sql: `select f.value = 'x', f.value::string = 'x' from lateral flatten(input => parse_json('["x"]')) f`,
		want: []string{"true|true"}},
	{name: "flatten key value", sql: `select c.key, sum(c.value) from (select parse_json('{"a": 2, "b": 3}') m union all select parse_json('{"a": 5}')) t, lateral flatten(input => t.m) c group by c.key order by 1`,
		want: []string{"a|7", "b|3"}},
	{name: "split_to_table", sql: `select index, value from table(split_to_table('a,b,,c', ','))`, want: []string{"1|a", "2|b", "3|", "4|c"}},
	{name: "generator", sql: `select seq4() s from table(generator(rowcount => 3)) order by s`, want: []string{"0", "1", "2"}},
	{name: "uniform", sql: `select min(u) >= 1 and max(u) <= 10 from (select uniform(1, 10, random()) u from table(generator(rowcount => 100)))`, want: []string{"true"}},

	// Clauses.
	{name: "nulls desc", sql: `select x from (values (1),(null),(3)) t(x) order by x desc`, want: []string{"NULL", "3", "1"}},
	{name: "nulls asc", sql: `select x from (values (1),(null),(3)) t(x) order by x`, want: []string{"1", "3", "NULL"}},
	{name: "pivot", sql: `select * from (select 'a' k, 1 v union all select 'b', 2) pivot (sum(v) for k in ('a', 'b'))`,
		want: []string{"1|2"}, cols: []string{"'a'", "'b'"}},
	{name: "unpivot", sql: `select * from (select 1 as col_a, 2 as "Col_B") unpivot (val for name in (col_a, "Col_B"))`,
		want: []string{"COL_A|1", "Col_B|2"}, cols: []string{"NAME", "VAL"}},
	{name: "values", sql: `select * from values (1, 'a'), (2, 'b')`, want: []string{"1|a", "2|b"}, cols: []string{"COLUMN1", "COLUMN2"}},
	{name: "minus", sql: `select 1 minus select 2`, want: []string{"1"}},
	{name: "top", sql: `select top 2 x from (values (1),(2),(3)) t(x) order by x`, want: []string{"1", "2"}},
	{name: "top distinct", sql: `select distinct top 2 x from (values (1),(1),(2),(3)) t(x) order by x`, want: []string{"1", "2"}},
	{name: "recursive", sql: `with r (n) as (select 1 union all select n + 1 from r where n < 3) select * from r`, want: []string{"1", "2", "3"}},
	{name: "star", sql: `select * exclude (cd) rename (ab as xy) from (select 1 as ab, 2 as cd, 3 as ef)`, want: []string{"1|3"}, cols: []string{"XY", "EF"}},
	// Snowflake lets a table alias be a word DuckDB reserves (AT, LEFT, MAP);
	// as a column's qualifier DuckDB reads it only quoted.
	{name: "keyword aliases", sql: `select at.x, left.y, map.z, AT.x + Left.y, at.* from (select 1 x) as at, (select 2 y) as left, (select parse_json('{"at": 3}') z) as map where map.z:at = 3`,
		want: []string{`1|2|{"at":3}|3|1`}, cols: []string{"X", "Y", "Z", "AT.X + LEFT.Y", "X"}},
	{name: "group by alias", sql: `select x + 1 as y, count(*) from (values (1),(2)) t(x) group by y order by y`, want: []string{"2|1", "3|1"}, cols: []string{"Y", "COUNT(*)"}},
	{name: "sample rows", sql: `select count(*) from (select * from range(100) t(x) sample (10 rows))`, want: []string{"10"}},
	{name: "column names", sql: `select x as y, x as "y", x as "Mixed", x::varchar, iff(x > 0, 'a', 'b'), upper('q'), x   +   1, "X" from (select 1 as x)`,
		want: []string{"1|1|1|1|a|Q|2|1"}, cols: []string{"Y", "y", "Mixed", "X::VARCHAR", "IFF(X > 0, 'A', 'B')", "UPPER('Q')", "X   +   1", "X"}},
	{name: "path names", sql: `select v:a, v:a::string, v['b'], v from (select parse_json('{"a":1,"b":2}') v)`,
		want: []string{`1|1|2|{"a":1,"b":2}`}, cols: []string{"V:A", "V:A::STRING", "V['B']", "V"}},
	{name: "star names", sql: `select t.x, t.* from (select 1 as x, 2 as "lower") t`, want: []string{"1|1|2"}, cols: []string{"X", "X", "lower"}},
	{name: "star replace", sql: `select * exclude (a) replace (b + 1 as b) from (select 1 as a, 2 as b, 3 as c)`, want: []string{"3|3"}, cols: []string{"B", "C"}},
	{name: "asof join", sql: `select t.id, q.px from (values (1, 10), (2, 20), (3, 5)) t(id, ts) asof join (values (8, 'a'), (15, 'b')) q(ts, px) match_condition (t.ts >= q.ts) order by t.id`,
		want: []string{"1|a", "2|b", "3|NULL"}},
	{name: "asof join on", sql: `select t.id, q.px from (values (1, 'x', 10), (2, 'y', 20)) t(id, k, ts) asof join (values ('x', 8, 'a'), ('x', 15, 'b'), ('y', 30, 'c')) q(k, ts, px) match_condition (t.ts >= q.ts) on t.k = q.k order by t.id`,
		want: []string{"1|a", "2|NULL"}},
	{name: "binary literals", sql: `select hex_encode(x'0a0B'), hex_encode(X'0a0B', 0), x'41' = to_binary('41', 'hex')`, want: []string{"0A0B|0a0b|true"}},
	{name: "time_from_parts", sql: `select time_from_parts(12, 34, 56, 987654321), time_from_parts(0, 100, 0), time_from_parts(25, 0, 0)`,
		want: []string{"12:34:56.987654|01:40:00|01:00:00"}},
	{name: "approx_count_distinct", sql: `select hll(a, b), approx_count_distinct(a) from (values (1, 2), (1, 2), (1, 3), (null, 1)) t(a, b)`, want: []string{"2|1"}},

	// TO_VARCHAR without a format, in Snowflake's default output formats.
	// A FLOAT has up to 12 significant digits: 999.889980465 and 9244 are
	// what Snowflake returned for two columns (October 21, 2026), where
	// DuckDB would print 999.8899804651736 and 9244.0.
	{name: "to_varchar floats", sql: `select to_varchar(999.8899804651736::float), to_varchar(9244::float), to_varchar(1::float / 3), to_char(1e20::float), to_varchar('nan'::float), to_varchar('-inf'::float), to_varchar(1e3), (-0.0001234)::float::varchar, to_varchar(123456789012::float), to_varchar(1234567890123::float)`,
		want: []string{"999.889980465|9244|0.333333333333|1e+20|NaN|-inf|1000|-0.0001234|123456789012|1.23456789012e+12"}},
	// TIMESTAMP_OUTPUT_FORMAT (LTZ and TZ) is YYYY-MM-DD HH24:MI:SS.FF3
	// TZHTZM, TIMESTAMP_NTZ_OUTPUT_FORMAT the same without the offset,
	// TIME_OUTPUT_FORMAT HH24:MI:SS.
	{name: "to_varchar timestamps", sql: `select to_varchar('2026-10-07 15:58:37.862 +00:00'::timestamp_ltz), to_varchar('2026-10-07 15:58:37.862'::timestamp_ntz), '2026-10-07 15:58:37.862123 +00:00'::timestamp_tz::varchar, to_varchar('08:58:37.862'::time), to_varchar('2026-10-07'::date)`,
		want: []string{"2026-10-07 15:58:37.862 +0000|2026-10-07 15:58:37.862|2026-10-07 15:58:37.862 +0000|08:58:37|2026-10-07"}},
	// NUMBER keeps its scale; BINARY_OUTPUT_FORMAT is HEX.
	{name: "to_varchar numbers and binary", sql: `select to_varchar(-0.5::number(10,2)), to_varchar(12345678901234567890::number(38,0)), to_varchar(42), 1.50::varchar, to_varchar(x'0a0b'), to_varchar(true)`,
		want: []string{"-0.50|12345678901234567890|42|1.50|0A0B|true"}},
	{name: "to_char offsets", sql: `select to_char('2026-10-07 15:58:37.862 +00:00'::timestamp_ltz, 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), to_char('2026-10-07 15:58:37'::timestamp_ltz, 'TZHTZM'), to_char('2026-10-07 15:58:37'::timestamp_ltz, 'HH24 "TZH" tzh')`,
		want: []string{"2026-10-07 15:58:37 +00:00|+0000|15 TZH +00"}},
	{name: "date_part from", sql: `select date_part(year from '2024-04-08'::date), extract('month', '2024-04-08'::date)`, want: []string{"2024|4"}},
	{name: "path of a cast", sql: `select parse_json('[{"a": 1}]')::variant[0]:a, '{"b": "x"}'::variant:b`, want: []string{`1|NULL`},
		cols: []string{`PARSE_JSON('[{"A": 1}]')::VARIANT[0]:A`, `'{"B": "X"}'::VARIANT:B`}},
	{name: "bool functions", sql: `select booland(1, -2), booland(0, 2.35), booland(0, null), booland(1, null), boolor(1, null), boolor(0, null), boolxor(2, 0), boolxor(1, -1), boolxor(0, null), boolnot(0), getbit(11, 0), getbit(11, 2)`,
		want: []string{"true|false|false|NULL|true|NULL|true|false|NULL|true|1|0"}},
	{name: "decode binary", sql: `select hex_decode_binary('4142') = x'4142', base64_decode_binary('QUI=') = x'4142'`, want: []string{"true|true"}},
	{name: "to_binary", sql: `select hex_encode(to_binary('SGVsbG8=', 'BASE64')), hex_encode(to_binary('Hello', 'UTF-8')), try_to_binary('zz'), hex_encode(to_binary('4142')), x'41'`,
		want: []string{`48656C6C6F|48656C6C6F|NULL|4142|\x41`}, cols: []string{"HEX_ENCODE(TO_BINARY('SGVSBG8=', 'BASE64'))", "HEX_ENCODE(TO_BINARY('HELLO', 'UTF-8'))", "TRY_TO_BINARY('ZZ')", "HEX_ENCODE(TO_BINARY('4142'))", "X'41'"}},
	// ARRAY_FLATTEN takes an ARRAY of ARRAYs one level down; a NULL element
	// makes it NULL, and an element that is not an ARRAY an error.
	{name: "array_flatten", sql: `select array_flatten([[1,2],[3]]), array_flatten([[1,[2]],[],[3]]), array_flatten([]), array_flatten([['a','b'],['c']]), array_flatten(null), array_flatten([[1], null])`,
		want: []string{`[1,2,3]|[1,[2],3]|[]|["a","b","c"]|NULL|NULL`}},
	{name: "array_flatten of a scalar", sql: `select array_flatten([[1], 2])`, wantErr: "not an array of arrays"},

	// In an ORDER BY expression a select list's alias comes before a column
	// of the same name, and a name both joined tables have is the alias.
	{name: "order by alias in an expression", sql: `select x + 1 as a from (select 1 as a, 10 as x union all select 2, 20) order by case a when 1 then 1 when 2 then 2 when 11 then 4 when 21 then 3 end`,
		want: []string{"21", "11"}},
	{name: "order by alias after top", sql: `select top 2 x + 1 as a from (select 1 as a, 10 as x union all select 2, 20 union all select 3, 30) order by case a when 11 then 3 when 21 then 2 when 31 then 1 end`,
		want: []string{"31", "21"}},
	{name: "order by alias of joined columns", sql: `select coalesce(p.k, q.k) as k from (select 1 as k union all select 3) p full join (select 2 as k union all select 3) q on p.k = q.k order by case k when 1 then 3 when 2 then 1 when 3 then 2 end`,
		want: []string{"2", "3", "1"}},

	// Implicit conversions (Options.Implicit): string functions read any
	// value as TO_VARCHAR writes it, LIKE does too, and text compared with
	// a timestamp or a number is cast to it.
	{name: "string functions of other types", implicit: true, sql: `select trim(n), upper(d), length(dt), trim(f), upper(b), lower(b2), length(ts) from (select 5 as n, 5.50::number(10,2) as d, '2024-01-01'::date as dt, 1.5::float as f, true as b, false as b2, '2024-01-01 10:00:00'::timestamp as ts)`,
		want: []string{"5|5.50|10|1.5|TRUE|false|23"}},
	{name: "string functions of numbers", implicit: true, sql: `select contains(n, '2'), startswith(n, '1'), replace(m, '5', 'x'), left(k, 2), substr(k, 2, 2), split_part(k, 3, 1) from (select 123 as n, 55 as m, 12345 as k)`,
		want: []string{"true|true|xx|12|23|12"}},
	{name: "like of other types", implicit: true, sql: `select n like '5%', d like '2024%', d not like '1999%' from (select 5 as n, '2024-01-01'::date as d)`,
		want: []string{"true|true|true"}},
	{name: "text compared with timestamps and numbers", implicit: true, sql: `select v between a and b, v >= a, v not between b and b, s > i, s > t2 from (select '2024-01-02 10:00:00'::varchar as v, '2024-01-01'::timestamp as a, '2024-01-03'::timestamp as b, '10'::varchar as s, 9 as i, '9'::varchar as t2)`,
		want: []string{"true|true|true|true|false"}},
}
