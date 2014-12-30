package com.hortonworks.skumpf.datetime;

import kafka.utils.Time;

/**
 * kafka.utils.Time is an interface and kafka.utils.SystemTime is private
 * Implementing kafka.util.Time to allow for public consumption by kafka.server.KafkaServer
 */
public class LocalSystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
        // no stress
        }
    }

}
