package org.fife.pgperf;

import java.sql.*;
import java.util.Random;

class PgUtil {

    private Connection conn;
    private Random random;

    private PreparedStatement insertStmt;
    private PreparedStatement copyRowStmt;

    PgUtil() throws SQLException {

        conn = createConnection();
        random = new Random();
    }

    void createTempTableClone(String tableName, String tempTableName, long rowCount, long pkStartOffs) throws SQLException {

        StringBuilder sb = new StringBuilder("create temp table \"" + tempTableName + "\" (");

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select * from \"" + tableName + "\" limit 1")) {

            ResultSetMetaData rsmd = rs.getMetaData();

            for (int i = 0; i < rsmd.getColumnCount(); i++) {

                String type = rsmd.getColumnTypeName(i + 1);
                if ("varchar".equalsIgnoreCase(type)) {
                    int len = rsmd.getColumnDisplaySize(i + 1);
                    if (len < Integer.MAX_VALUE) {
                        type += "(" + len + ")";
                    }
                }
                if (ResultSetMetaData.columnNoNulls == rsmd.isNullable(i + 1)) {
                    type += " not null";
                }

                String columnName = rsmd.getColumnName(i + 1);
                sb.append(columnName).append(' ').append(type);

                if (i < rsmd.getColumnCount() - 1) {
                    sb.append(", ");
                }
            }

            sb.append(");");

            System.out.println("Creating temp table:");
            System.out.println(sb.toString());
            stmt.execute(sb.toString());
        }

        insertAllDataTypesRecords(tempTableName, rowCount, pkStartOffs, tableName);
    }

    void createAllDataTypesTable(String tableName, long rowCount, long pkStartOffs) throws SQLException {

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists \"" + tableName + "\"");
        }

        String sql = "create table \"" + tableName + "\" (" +
                "bigint_col bigint unique not null, " +
                "bool_col bool, " +
                "float_col float, " +
                "date_col date, " +
                "double_precision_col double precision, " +
                "int_col integer, " +
                "bigserial_col bigserial, " +
                "text_col text, " +
                "timestamp_col timestamp, " +
                "timestamp_with_timezone_col timestamp with time zone, " +
                "timestamp_without_timezone_col timestamp without time zone, " +
                "varchar_unbounded_col varchar, " +
                "varchar_40_col varchar(40) not null" +
                ");";

        System.out.println("Creating initial table with schema:\n" + sql);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }

        insertAllDataTypesRecords(tableName, rowCount, pkStartOffs, null);
    }

    private static Connection createConnection() throws SQLException {

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        return DriverManager.getConnection(url, user, password);
    }

    private void insertAllDataTypesRecords(String tableName, long rowCount, long pkStartOffs, String randomlyCopyFrom) throws SQLException {

        insertStmt = conn.prepareStatement("insert into \"" + tableName
                + "\" values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        if (randomlyCopyFrom != null) {
            copyRowStmt = conn.prepareStatement("select * from \"" + randomlyCopyFrom + "\" offset ? limit 1");
        }

        conn.setAutoCommit(false);
        try {

            for (long i = 0; i < rowCount; i++) {

                if (i % 5 == 0 && randomlyCopyFrom != null) {
                    copyRow(i);
                }
                else {
                    insertAllDataTypesRecordImpl(pkStartOffs + i);
                }

                if (i % 200 == 0) {
                    insertStmt.executeBatch();
                }
            }

            // Cleanup batch execute
            insertStmt.executeBatch();
            conn.commit();

        } finally {
            conn.setAutoCommit(true);
        }
    }

    private void insertAllDataTypesRecordImpl(long pk) throws SQLException {

        insertStmt.setLong(1, pk);
        insertStmt.setBoolean(2, false);
        insertStmt.setFloat(3, 0);
        insertStmt.setDate(4, new Date(random.nextInt(50000)));
        insertStmt.setDouble(5, 0);
        insertStmt.setInt(6, 0);
        insertStmt.setLong(7, pk);
        insertStmt.setString(8, "a" + random.nextInt());
        insertStmt.setTimestamp(9, new Timestamp(random.nextInt(50000)));
        insertStmt.setTimestamp(10, new Timestamp(random.nextInt(50000)));
        insertStmt.setTimestamp(11, new Timestamp(random.nextInt(50000)));
        insertStmt.setString(12, "a" + random.nextInt());
        insertStmt.setString(13, "a" + random.nextInt());

        insertStmt.addBatch();
    }

    /**
     * Creates a row that shares a PK with an existing row, but the data is different (to make it easy to see the
     * upsert).
\     */
    private void copyRow(long row) throws SQLException {

        copyRowStmt.setLong(1, row);

        try (ResultSet rs = copyRowStmt.executeQuery()) {

            rs.next();

            insertStmt.setLong(1, rs.getLong(1));
            insertStmt.setBoolean(2, true);
            insertStmt.setFloat(3, 9999);
            insertStmt.setDate(4, null);
            insertStmt.setDouble(5, 9999);
            insertStmt.setInt(6, 9999);
            insertStmt.setLong(7, 9999);
            insertStmt.setString(8, "ZZZ");
            insertStmt.setTimestamp(9, null);
            insertStmt.setTimestamp(10, null);
            insertStmt.setTimestamp(11, null);
            insertStmt.setString(12, "ZZZ");
            insertStmt.setString(13, "ZZZ");

            insertStmt.addBatch();
        }
    }

    void performUpsert(String toTable, String fromTable) throws SQLException {

        StringBuilder sb = new StringBuilder("update \"" + toTable + "\" set ");
        ResultSetMetaData rsmd = null;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select * from \"" + toTable + "\" limit 1")) {

            rsmd = rs.getMetaData();
            boolean first = true;

            for (int i = 0; i < rsmd.getColumnCount(); i++) {

                String columnName = rsmd.getColumnName(i + 1);
//                System.out.println(columnName + " readOnly? - " + rsmd.isReadOnly(i + 1));
//                System.out.println(columnName + " writable? - " + rsmd.isWritable(i + 1));

                if (!rsmd.isReadOnly(i + 1)) {

                    if (!first) {
                        sb.append(", ");
                    }

                    first = false;

                    sb.append(columnName).append(" = \"").append(fromTable).append("\".").append(columnName);
                }
            }
        }

        sb.append(String.format(" from \"%s\" where \"%s\".bigint_col = \"%s\".bigint_col", fromTable, fromTable, toTable));

        System.out.println("Upsert step 1 of 2: Merge existing records sql:");
        System.out.println(sb.toString());
        System.out.println();

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sb.toString());
        }

        // Now insert totally new rows
        sb = new StringBuilder("insert into \"" + toTable + "\" select ");

        for (int i = 0; i < rsmd.getColumnCount(); i++) {

            String columnName = rsmd.getColumnName(i + 1);
            sb.append('"').append(fromTable).append("\".").append(columnName);

            if (i < rsmd.getColumnCount() - 1) {
                sb.append(", ");
            }
        }

        sb.append(" from \"" + fromTable + "\" ")
                .append("left outer join \"" + toTable + "\" on (\"" + toTable + "\".bigint_col = \"" + fromTable + "\".bigint_col) ")
                .append("where \"" + toTable + "\".bigint_col is null");

        System.out.println("Upsert step 2 of 2: Insert new records sql:");
        System.out.println(sb.toString());

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sb.toString());
        }
    }
}
