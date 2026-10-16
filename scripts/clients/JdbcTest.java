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
}
