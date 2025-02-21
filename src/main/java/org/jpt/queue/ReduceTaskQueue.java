package org.jpt.queue;

import org.jpt.model.ReduceTask;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Потокобезопасная очередь для работы с {@link ReduceTask}.
 */
public class ReduceTaskQueue {
    /**
     * Внутренняя реализация очереди.
     */
    private final BlockingQueue<ReduceTask> reduceTasks = new LinkedBlockingQueue<>();

    /**
     * Добавить задачу в очередь.
     *
     * @param task задача для добавления
     * @return {@code true} если задача была добавлена
     */
    public boolean offerTask(ReduceTask task) {
        return reduceTasks.offer(task);
    }

    /**
     * Взять задачу из очереди.
     * Может вернуть {@code null}.
     *
     * @return задача из очереди
     */
    public ReduceTask pollTask() {
        return reduceTasks.poll();
    }
}
