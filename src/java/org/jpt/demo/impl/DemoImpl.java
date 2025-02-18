package org.jpt.demo.impl;

import lombok.extern.slf4j.Slf4j;
import org.jpt.buffer.ConcurrentCircularBuffer;
import org.jpt.demo.Demo;
import org.jpt.util.Consumer;
import org.jpt.util.Producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
public class DemoImpl implements Demo {
    private final int ITEMS_AMOUNT = 100000;
    private final int CONSUMER_THREAD_AMOUNT = 5;
    private final int THREAD_EXPECTED = ITEMS_AMOUNT / CONSUMER_THREAD_AMOUNT;

    @Override
    public void run() {
        /*
        Не использую try-with-resources для ExecutorService из-за ошибки jdk.
        В чем дело пока не разобрался.
        java: incompatible types: java.util.concurrent.ExecutorService cannot be converted to java.lang.AutoCloseable
        Строчка не компилируется:
        AutoCloseable autoCloseable = Executors.newFixedThreadPool(1);
         */
        ExecutorService pool = null;
        try {
            pool = Executors.newFixedThreadPool(3);
            log.info("Demo starts.");
            ConcurrentCircularBuffer<Integer> buffer = new ConcurrentCircularBuffer<>(0);
            Integer[] items = IntStream.range(0, ITEMS_AMOUNT)
                    .limit(ITEMS_AMOUNT)
                    .boxed()
                    .toArray(Integer[]::new);

            Thread producerThread = new Thread(new Producer<>(buffer, items));
            Thread consumerThread = new Thread(new Consumer<>(buffer, THREAD_EXPECTED));

            for (int i = 0; i < CONSUMER_THREAD_AMOUNT; i++) {
                pool.submit(consumerThread);
            }
            producerThread.start();

            try {
                while (pool.awaitTermination(1, TimeUnit.SECONDS)) {
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } finally {
            if (pool != null) pool.shutdown();
        }
        log.info("Demo ends.");
    }
}
