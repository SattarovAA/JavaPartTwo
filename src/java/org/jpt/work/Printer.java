package org.jpt.work;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Создает поток для вывода итерируемого значения {@link #counter} в консоль.
 * {@link #valueType} = 1 - нечетные значения.
 * {@link #valueType} = 0 - четные значения.
 * Завершается при достижении {@link #LIMIT} значения.
 */
public class Printer implements Runnable {
    /**
     * Ограничение максимального значения.
     */
    private static final int LIMIT = 2_000_000;
    /**
     * Период вывода значения.
     */
    private static final int PERIOD = 2;
    /**
     * Lock для работы с {@link #counter}.
     */
    private static final Lock LOCK = new ReentrantLock();
    /**
     * Счетчик для вывода и итерации.
     */
    private final AtomicInteger counter;
    /**
     * Тип значения для вывода.
     */
    private final int valueType;

    public Printer(AtomicInteger counter, int valueType) {
        this.counter = counter;
        this.valueType = valueType;
    }

    @Override
    public void run() {
        while (counter.get() < LIMIT) {
            if (LOCK.tryLock()) {
                if (counter.get() % PERIOD == valueType) {
                    printValue();
                }
                LOCK.unlock();
            }
        }
    }

    /**
     * Выводит значение {@link #counter} и имя потока в консоль.
     */
    private void printValue() {
        System.out.println(counter.getAndIncrement()
                + Thread.currentThread().getName()
        );
    }
}
