import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks Igloo through the PostgreSQL JDBC driver, the way BI tools use it:
 * metadata calls for schema browsing, parameters, typed results, and paging
 * through a large result with a fetch size. Expects the TPC-H tables.
 * Run from scripts/e2e.sh.
 */
public class JdbcTest {
    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
        System.out.println("  jdbc: " + what);
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://" + System.getenv("PGHOST") + ":5432/igloo";
        try (Connection c = DriverManager.getConnection(url, System.getenv("PGUSER"), System.getenv("PGPASSWORD"))) {
            DatabaseMetaData md = c.getMetaData();

            List<String> tables = new ArrayList<>();
            try (ResultSet rs = md.getTables(null, "public", "%", new String[] {"TABLE"})) {
                while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
            }
            check(tables.contains("orders") && tables.contains("lineitem"), "getTables lists the TPC-H tables");

            List<String> columns = new ArrayList<>();
            try (ResultSet rs = md.getColumns(null, "public", "orders", "%")) {
                while (rs.next()) columns.add(rs.getString("COLUMN_NAME") + ":" + rs.getString("TYPE_NAME"));
            }
            check(columns.size() == 9 && columns.contains("o_totalprice:numeric") && columns.contains("o_orderdate:date"),
                "getColumns describes every column " + columns);

            // Comments and views, as schema browsers show them.
            try (Statement st = c.createStatement()) {
                st.execute("COMMENT ON TABLE region IS 'Five regions'");
                st.execute("COMMENT ON COLUMN region.r_name IS 'Region name'");
                st.execute("CREATE OR REPLACE VIEW jdbc_regions AS SELECT r_name FROM region");
            }
            try (ResultSet rs = md.getTables(null, "public", "region", new String[] {"TABLE"})) {
                check(rs.next() && "Five regions".equals(rs.getString("REMARKS")), "getTables reports table comments");
            }
            try (ResultSet rs = md.getColumns(null, "public", "region", "r_name")) {
                check(rs.next() && "Region name".equals(rs.getString("REMARKS")), "getColumns reports column comments");
            }
            List<String> views = new ArrayList<>();
            try (ResultSet rs = md.getTables(null, "public", "%", new String[] {"VIEW"})) {
                while (rs.next()) views.add(rs.getString("TABLE_NAME"));
            }
            check(views.contains("jdbc_regions"), "getTables lists views " + views);
            try (Statement st = c.createStatement()) {
                st.execute("DROP VIEW jdbc_regions");
            }

            try (PreparedStatement ps = c.prepareStatement("SELECT r_name FROM region WHERE r_regionkey = ?")) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    check(rs.next() && rs.getString(1).equals("ASIA"), "prepared statement with a parameter");
                }
            }

            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT o_orderdate, o_totalprice FROM orders ORDER BY o_orderkey LIMIT 1")) {
                check(rs.next() && rs.getDate(1) != null && rs.getBigDecimal(2).scale() == 2, "dates and decimals");
            }

            biToolMetadata(c, md);
            biToolQueries(c);

            long expected;
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT count(*) FROM lineitem")) {
                rs.next();
                expected = rs.getLong(1);
            }
            // A fetch size makes the driver page through results with
            // suspended portals, as DBeaver and other tools do.
            c.setAutoCommit(false);
            long n = 0;
            try (Statement st = c.createStatement()) {
                st.setFetchSize(5000);
                try (ResultSet rs = st.executeQuery("SELECT l_orderkey FROM lineitem")) {
                    while (rs.next()) n++;
                }
            }
            c.commit();
            check(n == expected, "fetch size paging returned all " + n + " rows");
        }
    }

    /**
     * The metadata Tableau and Looker read through this driver when they
     * connect, list schemas and tables, and describe a query's columns
     * before running it.
     */
    static void biToolMetadata(Connection c, DatabaseMetaData md) throws SQLException {
        check(md.getDatabaseProductName().equals("PostgreSQL") && md.getDatabaseMajorVersion() >= 15
            && "\"".equals(md.getIdentifierQuoteString()), "product, version and identifier quoting");
        check(c.getTransactionIsolation() == Connection.TRANSACTION_REPEATABLE_READ, "transaction isolation level");
        c.setReadOnly(true);
        c.setReadOnly(false);
        check(rowCount(md.getCatalogs()) >= 1 && strings(md.getSchemas(), "TABLE_SCHEM").contains("public"), "getCatalogs and getSchemas");
        check(strings(md.getTableTypes(), "TABLE_TYPE").containsAll(List.of("TABLE", "VIEW")), "getTableTypes");
        check(strings(md.getTables(null, null, "%", null), "TABLE_NAME").contains("orders"), "getTables of every schema and type");
        try (ResultSet rs = md.getColumns(null, "public", "orders", "o_totalprice")) {
            check(rs.next() && rs.getInt("DATA_TYPE") == Types.NUMERIC && rs.getInt("COLUMN_SIZE") == 15
                && rs.getInt("DECIMAL_DIGITS") == 2,
                "getColumns reports a decimal's precision and scale");
        }
        // Iceberg tables have no keys or indexes to report, but tools ask.
        rowCount(md.getPrimaryKeys(null, "public", "orders"));
        rowCount(md.getImportedKeys(null, "public", "orders"));
        rowCount(md.getExportedKeys(null, "public", "orders"));
        rowCount(md.getIndexInfo(null, "public", "orders", false, true));
        check(rowCount(md.getTypeInfo()) > 10, "keys, indexes and getTypeInfo");

        // Describing a query without running it: a prepared statement's
        // metadata, and a query wrapped in LIMIT 0.
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT o_orderkey, o_totalprice, o_orderdate, o_orderpriority FROM orders WHERE o_custkey = ?")) {
            ResultSetMetaData rmd = ps.getMetaData();
            check(rmd.getColumnCount() == 4 && isInteger(rmd.getColumnType(1))
                && rmd.getColumnType(2) == Types.NUMERIC && rmd.getPrecision(2) == 15 && rmd.getScale(2) == 2
                && rmd.getColumnType(3) == Types.DATE && rmd.getColumnType(4) == Types.VARCHAR
                && isInteger(ps.getParameterMetaData().getParameterType(1)),
                "prepared statement metadata before running it");
        }
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM (SELECT o_orderpriority AS \"Priority\", sum(o_totalprice) AS \"Revenue\" "
                 + "FROM orders GROUP BY 1) \"Custom SQL Query\" LIMIT 0")) {
            ResultSetMetaData rmd = rs.getMetaData();
            check(!rs.next() && rmd.getColumnCount() == 2 && rmd.getColumnLabel(1).equals("Priority")
                && rmd.getColumnType(2) == Types.NUMERIC, "LIMIT 0 describes a custom SQL query");
        }
    }

    /** Queries shaped like the SQL Tableau and Looker generate. */
    static void biToolQueries(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            // Tableau: a date dimension truncated to months, and a measure.
            BigDecimal total = BigDecimal.ZERO;
            int months = 0;
            try (ResultSet rs = st.executeQuery("SELECT CAST(DATE_TRUNC('MONTH', CAST(\"orders\".\"o_orderdate\" AS DATE)) AS DATE) "
                    + "AS \"tmn:o_orderdate:ok\", SUM(\"orders\".\"o_totalprice\") AS \"sum:o_totalprice:ok\" "
                    + "FROM \"public\".\"orders\" \"orders\" GROUP BY 1 ORDER BY 1")) {
                while (rs.next()) {
                    months++;
                    total = total.add(rs.getBigDecimal(2));
                }
            }
            BigDecimal expected;
            try (ResultSet rs = st.executeQuery("SELECT sum(o_totalprice) FROM orders")) {
                rs.next();
                expected = rs.getBigDecimal(1);
            }
            check(months == 80 && total.compareTo(expected) == 0, "Tableau: DATE_TRUNC and SUM by month");

            // Looker: a CTE, window functions, CASE and TO_CHAR.
            try (ResultSet rs = st.executeQuery("WITH monthly AS (SELECT DATE_TRUNC('month', o_orderdate) AS month, o_orderpriority, "
                    + "SUM(o_totalprice) AS revenue, COUNT(*) AS orders FROM orders GROUP BY 1, 2) "
                    + "SELECT TO_CHAR(month, 'YYYY-MM') AS \"orders.created_month\", o_orderpriority, revenue, "
                    + "SUM(revenue) OVER (PARTITION BY o_orderpriority ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running, "
                    + "RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rank_in_month, "
                    + "LAG(revenue) OVER (PARTITION BY o_orderpriority ORDER BY month) AS previous_month, "
                    + "CASE WHEN orders >= 400 THEN 'busy' ELSE 'quiet' END AS load "
                    + "FROM monthly ORDER BY 1, 2")) {
                int rows = 0;
                boolean ranked = true;
                String first = null;
                BigDecimal running = BigDecimal.ZERO;
                while (rs.next()) {
                    if (first == null) first = rs.getString(1);
                    ranked &= rs.getInt(5) >= 1 && rs.getInt(5) <= 5;
                    if (rs.getString(2).equals("1-URGENT")) running = rs.getBigDecimal(4);
                    rows++;
                }
                check(rows == 400 && "1992-01".equals(first) && ranked && running.signum() > 0,
                    "Looker: CTE, running totals, RANK, LAG, CASE and TO_CHAR");
            }

            // Percentiles and distinct counts, as box plots and KPIs use.
            try (ResultSet rs = st.executeQuery("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o_totalprice), "
                    + "PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY o_totalprice), COUNT(DISTINCT o_custkey), "
                    + "EXTRACT(YEAR FROM MAX(o_orderdate)) FROM orders")) {
                check(rs.next() && rs.getDouble(1) > 0 && rs.getDouble(2) > rs.getDouble(1) && rs.getLong(3) > 0
                    && rs.getInt(4) == 1998, "percentiles, COUNT(DISTINCT) and EXTRACT");
            }
        }
    }

    static boolean isInteger(int type) {
        return type == Types.INTEGER || type == Types.BIGINT;
    }

    static int rowCount(ResultSet rs) throws SQLException {
        try (rs) {
            int n = 0;
            while (rs.next()) n++;
            return n;
        }
    }

    static List<String> strings(ResultSet rs, String column) throws SQLException {
        try (rs) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(column));
            return out;
        }
    }
}
