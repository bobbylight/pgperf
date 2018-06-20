package org.fife.pgperf;

import java.sql.*;

public class Main {

    public static void printDataTypes(Connection conn, String tableName) throws SQLException {

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select * from \"" + tableName + "\" limit 1")) {

            ResultSetMetaData rsmd = rs.getMetaData();

            System.out.printf("Table \"%s\" has %d columns:\n", tableName, rsmd.getColumnCount());

            for (int i = 0; i < rsmd.getColumnCount(); i++) {

                String type = rsmd.getColumnTypeName(i + 1);
                if ("varchar".equalsIgnoreCase(type)) {
                    type += "(" + rsmd.getColumnDisplaySize(i + 1) + ")";
                }
                if (ResultSetMetaData.columnNoNulls == rsmd.isNullable(i + 1)) {
                    type += " not null";
                }

                System.out.printf("%d. %s %s\n", i + 1, rsmd.getColumnName(i + 1), type);
            }
        }
    }

    public static void createAllDataTypesTable(Connection conn, String tableName) throws SQLException {

        try (Statement stmt = conn.createStatement()) {

            stmt.execute("drop table if exists \"" + tableName + "\"");

            String sql = "create table \"" + tableName + "\" (\n" +
                    "   bigint_col bigint,\n" +
                    "   bool_col bool,\n" +
                    "   float_col float,\n" +
                    "   date_col date,\n" +
                    "   double_precision_col double precision,\n" +
                    "   int_col integer,\n" +
                    "   serial_col serial,\n" +
                    "   text_col text,\n" +
                    "   timestamp_col timestamp,\n" +
                    "   timestamp_with_timezone_col timestamp with time zone,\n" +
                    "   timestamp_without_timezone_col timestamp without time zone,\n" +
                    "   varchar_unbounded_col varchar,\n" +
                    "   varchar_40_col varchar(40) unique not null\n" +
                    ");";

            System.out.println("Creating table with schema:\n" + sql);

            stmt.execute(sql);
        }
    }

    public static Connection createConnection() throws SQLException {

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        return DriverManager.getConnection(url, user, password);
    }

    public static void main(String[] args) throws SQLException {

        try (Connection conn = createConnection()) {
            createAllDataTypesTable(conn, "test_table");
            printDataTypes(conn, "test_table");
        }
    }
}
