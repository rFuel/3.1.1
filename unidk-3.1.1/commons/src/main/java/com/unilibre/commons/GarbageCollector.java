package com.unilibre.commons;

public class GarbageCollector {

    private static int rubbishCollector = 60, gcSize = 50;
    private static long gcStart = System.nanoTime(), gcNow = System.nanoTime();
    private static double gcTimer, nanoSecs = 1000000000.00;

    public static void setStart(long value) { gcStart = value; }

    public static void setCollection(int value) {
        rubbishCollector = value;
    }

    public static int getGCsize() {
        return gcSize;
    }

    public static void CleanUp() {
        gcNow = System.nanoTime();
        gcTimer = ((gcNow - gcStart) / nanoSecs);
        if (gcTimer > (rubbishCollector)) {
            NamedCommon.MQgarbo.gc();
            gcStart = gcNow;
        }
    }
}
