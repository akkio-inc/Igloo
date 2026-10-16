// Checks Igloo through Npgsql the way Power BI's PostgreSQL connector uses
// it: loading types on connect, the navigator's information_schema queries,
// folded queries with parameters, column metadata and every data type.
// Expects the TPC-H tables. Connection settings come from the PG*
// environment variables. Run from scripts/bi-e2e.sh.
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using Npgsql;

static class Program
{
    static void Check(bool ok, string what)
    {
        if (!ok)
        {
            Console.WriteLine("  npgsql: FAILED " + what);
            Environment.Exit(1);
        }
        Console.WriteLine("  npgsql: " + what);
    }

    static List<object[]> Query(NpgsqlConnection conn, string sql, params NpgsqlParameter[] parameters)
    {
        using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddRange(parameters);
        using var reader = cmd.ExecuteReader();
        var rows = new List<object[]>();
        while (reader.Read())
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            rows.Add(row);
        }
        return rows;
    }

    static int Main()
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = Environment.GetEnvironmentVariable("PGHOST"),
            Port = 5432,
            Username = Environment.GetEnvironmentVariable("PGUSER"),
            Password = Environment.GetEnvironmentVariable("PGPASSWORD"),
            Database = "igloo",
        };
        using var conn = new NpgsqlConnection(builder.ConnectionString);
        conn.Open();
        Check(conn.PostgreSqlVersion.Major >= 15, $"Npgsql {typeof(NpgsqlConnection).Assembly.GetName().Version} connects and loads types");

        // Power BI's navigator lists tables, then a table's columns and
        // foreign keys.
        var tables = Query(conn, "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.tables " +
            "where TABLE_SCHEMA not in ('information_schema', 'pg_catalog') order by TABLE_SCHEMA, TABLE_NAME");
        Check(tables.Any(r => (string)r[0] == "public" && (string)r[1] == "orders" && (string)r[2] == "BASE TABLE")
            && tables.Any(r => (string)r[1] == "revenue_by_month" && (string)r[2] == "VIEW"), "navigator lists tables and views");
        var columns = Query(conn, "select COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, case when (data_type like '%unsigned%') " +
            "then DATA_TYPE || ' unsigned' else DATA_TYPE end as DATA_TYPE from INFORMATION_SCHEMA.columns " +
            "where TABLE_SCHEMA = 'public' and TABLE_NAME = 'orders' order by TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");
        Check(columns.Count == 9 && (string)columns[0][0] == "o_orderkey", "navigator describes columns");
        Query(conn, "select pkcol.COLUMN_NAME as PK_COLUMN_NAME, fkcol.TABLE_SCHEMA AS FK_TABLE_SCHEMA, " +
            "fkcol.TABLE_NAME AS FK_TABLE_NAME, fkcol.COLUMN_NAME as FK_COLUMN_NAME, fkcol.ORDINAL_POSITION as ORDINAL, " +
            "fkcon.CONSTRAINT_SCHEMA || '_' || fkcol.TABLE_NAME || '_' || 'orders' || '_' || fkcon.CONSTRAINT_NAME as FK_NAME " +
            "from (select distinct constraint_catalog, constraint_schema, unique_constraint_schema, constraint_name, " +
            "unique_constraint_name from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS) fkcon " +
            "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcol on fkcon.CONSTRAINT_SCHEMA = fkcol.CONSTRAINT_SCHEMA " +
            "and fkcon.CONSTRAINT_NAME = fkcol.CONSTRAINT_NAME " +
            "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkcol on fkcon.UNIQUE_CONSTRAINT_SCHEMA = pkcol.CONSTRAINT_SCHEMA " +
            "and fkcon.UNIQUE_CONSTRAINT_NAME = pkcol.CONSTRAINT_NAME " +
            "where pkcol.TABLE_SCHEMA = 'public' and pkcol.TABLE_NAME = 'orders' and pkcol.ORDINAL_POSITION = fkcol.ORDINAL_POSITION " +
            "order by FK_NAME, fkcol.ORDINAL_POSITION");
        Check(true, "navigator reads foreign keys");
        var schemaTables = conn.GetSchema("Tables");
        var schemaColumns = conn.GetSchema("Columns", new[] { null, "public", "orders" });
        Check(schemaTables.Rows.Count > 0 && schemaColumns.Rows.Count == 9,
            $"GetSchema lists tables and columns ({schemaTables.Rows.Count} tables, {schemaColumns.Rows.Count} columns of orders)");

        // Every type a report reads.
        using (var cmd = new NpgsqlCommand("select 1::smallint, 2::integer, 3::bigint, 1.25::numeric(10,2), 2.5::double precision, " +
            "'text'::varchar, true, date '2026-10-01', timestamp '2026-10-01 12:30:00', timestamptz '2026-10-01 12:30:00+00', " +
            "'8f4e8f0c-6a8d-4d6e-9f43-2d6c6f1b2b6e'::uuid, '\\x01\\x02'::blob, interval '1 day 2 hours', {'a': 1}::json", conn))
        using (var reader = cmd.ExecuteReader())
        {
            reader.Read();
            Check(reader.GetInt16(0) == 1 && reader.GetInt32(1) == 2 && reader.GetInt64(2) == 3 && reader.GetDecimal(3) == 1.25m
                && reader.GetDouble(4) == 2.5 && reader.GetString(5) == "text" && reader.GetBoolean(6)
                && reader.GetDateTime(7) == new DateTime(2026, 10, 1) && reader.GetDateTime(8) == new DateTime(2026, 10, 1, 12, 30, 0)
                && reader.GetDateTime(9).ToUniversalTime() == new DateTime(2026, 10, 1, 12, 30, 0, DateTimeKind.Utc)
                && reader.GetGuid(10) == Guid.Parse("8f4e8f0c-6a8d-4d6e-9f43-2d6c6f1b2b6e")
                && ((byte[])reader.GetValue(11)).SequenceEqual(new byte[] { 1, 2 })
                && reader.GetTimeSpan(12) == new TimeSpan(1, 2, 0, 0) && reader.GetString(13) == "{\"a\":1}",
                "integers, decimals, floats, text, booleans, dates, timestamps, UUIDs, bytes, intervals and JSON");
        }

        // A query Power BI folds from a report visual, with a parameter.
        var since = new NpgsqlParameter("since", new DateTime(1995, 1, 1));
        var folded = Query(conn, "select \"_\".\"o_orderpriority\" as \"o_orderpriority\", sum(\"_\".\"o_totalprice\") as \"a0\", " +
            "count(1) as \"a1\" from \"public\".\"orders\" \"_\" where \"_\".\"o_orderdate\" >= @since " +
            "group by \"_\".\"o_orderpriority\" order by \"_\".\"o_orderpriority\" limit 1000001", since);
        Check(folded.Count == 5 && folded.All(r => (decimal)r[1] > 0 && (long)r[2] > 0), "a folded aggregate with a parameter");

        // Column metadata of a result, which Power BI turns into its types.
        using (var cmd = new NpgsqlCommand("select o_orderkey, o_totalprice, o_orderdate, o_comment from orders limit 0", conn))
        using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
        {
            var schema = reader.GetSchemaTable();
            var price = schema.Rows[1];
            Check(schema.Rows.Count == 4 && (Type)price["DataType"] == typeof(decimal) && Convert.ToInt32(price["NumericPrecision"]) == 15
                && Convert.ToInt32(price["NumericScale"]) == 2
                // Npgsql 6 and later read dates as DateOnly, earlier ones as DateTime.
                && ((Type)schema.Rows[2]["DataType"] == typeof(DateTime) || (Type)schema.Rows[2]["DataType"] == typeof(DateOnly)),
                $"GetSchemaTable reports types, precision and scale ({price["DataType"]} {price["NumericPrecision"]},{price["NumericScale"]}; {schema.Rows[2]["DataType"]})");
        }

        // A prepared statement, run twice.
        using (var cmd = new NpgsqlCommand("select count(*) from lineitem where l_quantity > @q", conn))
        {
            cmd.Parameters.Add(new NpgsqlParameter("q", 10m));
            cmd.Prepare();
            var first = (long)cmd.ExecuteScalar();
            cmd.Parameters[0].Value = 40m;
            var second = (long)cmd.ExecuteScalar();
            Check(first > second && second > 0, "a prepared statement");
        }

        // Importing a whole table, as a report refresh does.
        var watch = Stopwatch.StartNew();
        long rows = 0;
        using (var cmd = new NpgsqlCommand("select * from \"public\".\"lineitem\"", conn))
        using (var reader = cmd.ExecuteReader())
        {
            var values = new object[reader.FieldCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                rows++;
            }
        }
        var expected = (long)new NpgsqlCommand("select count(*) from lineitem", conn).ExecuteScalar();
        Check(rows == expected, $"imports lineitem: {rows} rows in {watch.Elapsed.TotalSeconds:F1} s, {rows / watch.Elapsed.TotalSeconds:N0} rows/s");
        return 0;
    }
}
