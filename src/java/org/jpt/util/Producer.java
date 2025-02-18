package org.jpt.util;

import lombok.extern.slf4j.Slf4j;
import org.jpt.buffer.ConcurrentCircularBuffer;

/**
 * Util-class для добавления данных в {@link ConcurrentCircularBuffer}.
 *
 * @param <T> Тип данных в буфере
 */
@Slf4j
public class Producer<T> implements Runnable {
    /**
     * Буфер для добавления данных в него.
     */
    private final ConcurrentCircularBuffer<T> buffer;
    /**
     * Элементы для добавления в буфер.
     */
    private final T[] items;

    public Producer(ConcurrentCircularBuffer<T> buffer, T[] items) {
        this.buffer = buffer;
        this.items = items;
    }

    /**
     * Добавляет все элементы {@link #items} в {@link #buffer}.
     */
    @Override
    public void run() {
        for (int i = 0; i < items.length; ) {
            if (buffer.offer(items[i])) {
                log.info("Produced: " + items[i]);
                i++;
            }
        }
        log.info("Producer finished");
    }
}
