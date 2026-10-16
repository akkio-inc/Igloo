import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks Igloo's Arrow Flight SQL endpoint through the Apache Arrow Flight
 * SQL JDBC driver, the way BI tools use it: schema browsing, prepared
 * statements with parameters, typed results and a large result. Expects
 * the TPC-H tables. Run from scripts/e2e.sh, which sets FLIGHT_HOST,
 * FLIGHT_PORT and IGLOO_ADMIN_PASSWORD.
 */
public class FlightJdbcTest {
    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
        System.out.println("  flight jdbc: " + what);
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:arrow-flight-sql://" + System.getenv("FLIGHT_HOST") + ":" + System.getenv("FLIGHT_PORT")
            + "/?useEncryption=false";
        try (Connection c = DriverManager.getConnection(url, "igloo", System.getenv("IGLOO_ADMIN_PASSWORD"))) {
            DatabaseMetaData md = c.getMetaData();
            check("Igloo".equals(md.getDatabaseProductName()), "product name " + md.getDatabaseProductName());

            List<String> tables = new ArrayList<>();
            try (ResultSet rs = md.getTables(null, "public", "%", new String[] {"TABLE"})) {
                while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
            }
            check(tables.contains("orders") && tables.contains("lineitem"), "getTables lists the TPC-H tables");

            List<String> columns = new ArrayList<>();
            try (ResultSet rs = md.getColumns(null, "public", "orders", "%")) {
                while (rs.next()) columns.add(rs.getString("COLUMN_NAME"));
            }
            check(columns.size() == 9 && columns.contains("o_totalprice"), "getColumns describes every column " + columns);

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

            try (Statement st = c.createStatement()) {
                st.executeUpdate("CREATE TABLE IF NOT EXISTS flight_jdbc (i INTEGER)");
                int n = st.executeUpdate("INSERT INTO flight_jdbc VALUES (1), (2), (3)");
                check(n == 3, "executeUpdate reports " + n + " rows");
            }

            long n = 0;
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT l_orderkey FROM lineitem")) {
                while (rs.next()) n++;
            }
            check(n == 600572, "a large result returned all " + n + " rows");
        }
    }
}
