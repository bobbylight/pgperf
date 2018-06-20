package org.fife.pgperf;

public class Timer {

    private static long start;

    public static void start() {
        start = System.currentTimeMillis();
    }

    public static void stopAndLog(String operation) {
        long millis = System.currentTimeMillis() - start;
        System.out.printf("%s completed in %f seconds\n\n", operation, millis / 1000f);
    }
}
