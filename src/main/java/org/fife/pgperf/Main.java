package org.fife.pgperf;

import java.sql.*;

public class Main {

    private static final String TABLE_NAME = "test_table";

    private static final int MAIN_TABLE_ROW_COUNT = 1_000_000;
    private static final int NEW_DATA_ROW_COUNT = 100_000;

    public static void main(String[] args) throws SQLException {

        PgUtil pgUtil = new PgUtil();

        Timer.start();
        pgUtil.createAllDataTypesTable(TABLE_NAME, MAIN_TABLE_ROW_COUNT, 0);
        Timer.stopAndLog(String.format("Creating table %s with %d rows", TABLE_NAME, MAIN_TABLE_ROW_COUNT));

        Timer.start();
        pgUtil.createTempTableClone(TABLE_NAME, "temp_table", NEW_DATA_ROW_COUNT, 2*MAIN_TABLE_ROW_COUNT);
        Timer.stopAndLog(String.format("Creating temp table with same schema as %s, with %d rows", TABLE_NAME, NEW_DATA_ROW_COUNT));

        Timer.start();
        pgUtil.performUpsert(TABLE_NAME, "temp_table");
        Timer.stopAndLog(String.format("Merging %s into %s via left outer join", "temp_table", TABLE_NAME));
    }
}
