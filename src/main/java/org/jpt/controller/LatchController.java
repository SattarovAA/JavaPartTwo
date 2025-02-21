package org.jpt.controller;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

/**
 * Класс для синхронизации работы потоков по фазам
 * с помощью {@link CountDownLatch}.
 */
@Slf4j
public class LatchController {
    /**
     * Метка начала Map-фазы Worker-ов.
     */
    private final CountDownLatch startMapPhaseLatch;
    /**
     * Счетчик прогресса Map-фазы Worker-ов.
     */
    private CountDownLatch mapPhaseLatch;
    /**
     * Метка начала Reduce-фазы Worker-ов.
     */
    private final CountDownLatch startReducePhaseLatch;
    /**
     * Счетчик прогресса Reduce-фазы Worker-ов.
     */
    private final CountDownLatch reducePhaseLatch;

    public LatchController(int reduceAmount) {
        this.startMapPhaseLatch = new CountDownLatch(1);
        this.startReducePhaseLatch = new CountDownLatch(1);
        this.reducePhaseLatch = new CountDownLatch(reduceAmount);
    }

    /**
     * Инициализация счетчика прогресса Map-фазы.
     *
     * @param mapTaskAmount количество созданных {@code MapTask}
     */
    public void initMapPhaseLatch(int mapTaskAmount) {
        mapPhaseLatch = new CountDownLatch(mapTaskAmount);
    }

    /**
     * Индикатор окончания Map-фазы Worker-ов.
     *
     * @return {@code true} если счетчик выполненных задач фазы достиг нуля
     */
    public boolean isMapPhaseCompleted() {
        return mapPhaseLatch.getCount() <= 0;
    }

    /**
     * Индикатор окончания Reduce-фазы Worker-ов.
     *
     * @return {@code true} если счетчик выполненных задач фазы достиг нуля
     */
    public boolean isReducePhaseCompleted() {
        return reducePhaseLatch.getCount() <= 0;
    }

    /**
     * Ожидание начала Map-фазы Worker-ов.
     */
    public void awaitMapPhaseStart() {
        awaitLatch(startMapPhaseLatch);
    }

    /**
     * Ожидание окончания Map-фазы Worker-ов.
     */
    public void awaitMapPhaseEnd() {
        awaitLatch(mapPhaseLatch);
    }

    /**
     * Ожидание начала Reduce-фазы Worker-ов.
     */
    public void awaitReducePhaseStart() {
        awaitLatch(startReducePhaseLatch);
    }

    /**
     * Ожидание окончания Reduce-фазы Worker-ов.
     */
    public void awaitReducePhaseEnd() {
        awaitLatch(reducePhaseLatch);
    }

    /**
     * Ожидание окончания счетчика.
     *
     * @param latch счетчик для ожидания
     */
    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Метка начала Map-фазы Worker-ов.
     */
    public void startMapPhaseCall() {
        startMapPhaseLatch.countDown();
    }

    /**
     * Метка начала Reduce-фазы Worker-ов.
     */
    public void startReducePhaseCall() {
        startReducePhaseLatch.countDown();
    }

    /**
     * Метка выполнения Map-задачи Worker-ов.
     */
    public void mapTaskCompleteCall() {
        mapPhaseLatch.countDown();
    }

    /**
     * Метка выполнения Reduce-задачи Worker-ов.
     */
    public void reduceTaskCompleteCall() {
        reducePhaseLatch.countDown();
    }
}
