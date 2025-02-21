package org.jpt.util;

import lombok.extern.slf4j.Slf4j;
import org.jpt.buffer.ConcurrentCircularBuffer;

/**
 * Util-class для потребления данных из {@link ConcurrentCircularBuffer}.
 *
 * @param <T> Тип данных в буфере
 */
@Slf4j
public class Consumer<T> implements Runnable {
    /**
     * Буфер с данными для потребления.
     */
    private final ConcurrentCircularBuffer<T> buffer;
    /**
     * Количество элементов, которые поток ожидает забрать из буфера.
     */
    private final int expectedCount;

    public Consumer(ConcurrentCircularBuffer<T> buffer, int expectedCount) {
        this.buffer = buffer;
        this.expectedCount = expectedCount;
    }

    /**
     * Забирает {@link #expectedCount} элементов из {@link #buffer}.
     */
    @Override
    public void run() {
        for (int i = 0; i < expectedCount; ) {
            T item = buffer.poll();
            if (item != null) {
                i++;
                log.info("Consumed: " + item);
            }
        }
        log.info("Consumer finished");
    }
}
