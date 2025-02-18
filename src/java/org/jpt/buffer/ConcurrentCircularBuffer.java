package org.jpt.buffer;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ring buffer для работы в многопоточной среде.
 *
 * @param <E> element type
 */
public class ConcurrentCircularBuffer<E> {
    /**
     * Размер буфера по умолчанию.
     */
    private static final int DEFAULT_CAPACITY = 100;
    /**
     * Пустой массив для инициации пустого буфера.
     */
    private static final Object[] EMPTY_DATA = {};
    /**
     * Lock для работы с данными буфера.
     */
    private final Lock lock = new ReentrantLock();
    /**
     * Размер буфера.
     */
    private int capacity;
    /**
     * Данные буфера.
     */
    private Object[] data;
    /**
     * Указатель чтения.
     */
    private volatile int readSequence;
    /**
     * Указатель записи.
     */
    private volatile int writeSequence;

    public ConcurrentCircularBuffer() {
        this.data = EMPTY_DATA;
        this.capacity = 0;
        this.readSequence = 0;
        this.writeSequence = -1;
    }

    public ConcurrentCircularBuffer(E[] arr) {
        this();
        this.capacity = arr.length;
        this.data = arr;
        this.writeSequence += arr.length;
    }

    public ConcurrentCircularBuffer(int capacity) {
        this();
        this.capacity = (capacity < 1) ? DEFAULT_CAPACITY : capacity;
        this.data = new Object[this.capacity];
    }


    /**
     * Операция добавления в буфер.
     * В случае если буфер заполнен - не добавляет элемент в буфер.
     *
     * @param element объект для добавления
     * @return {@code false} if buffer is full
     */
    public boolean offer(E element) {
        lock.lock();
        try {
            if (!isFull()) {
                int nextWriteSeq = writeSequence + 1;
                data[nextWriteSeq % capacity] = element;
                writeSequence++;
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * Операция взятия из буфера.
     * В случае если буфер пустой - возвращает {@code null}.
     *
     * @return элемент, который был добавлен в буфер
     */
    @SuppressWarnings("unchecked")
    public E poll() {
        lock.lock();
        try {
            if (!isEmpty()) {
                E nextValue = (E) data[readSequence % capacity];
                readSequence++;
                return nextValue;
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * Возвращает {@code true} если буфер содержит данные.
     *
     * @return {@code true} if empty
     */
    public boolean isEmpty() {
        return writeSequence < readSequence;
    }

    /**
     * Возвращает {@code true} если в буфере нет пустого места.
     *
     * @return {@code true} if full
     */
    public boolean isFull() {
        return getSize() == capacity;
    }

    /**
     * Возвращает количество элементов в буфере.
     *
     * @return количество элементов в буфере
     */
    public int getSize() {
        return (writeSequence - readSequence) + 1;
    }
}
