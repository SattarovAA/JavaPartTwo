package org.jpt.queue;

import org.jpt.model.MapTask;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Потокобезопасная очередь для работы с {@link MapTask}.
 */
public class MapTaskQueue {
    /**
     * Внутренняя реализация очереди.
     */
    private final BlockingQueue<MapTask> mapTasks = new LinkedBlockingQueue<>();

    /**
     * Добавить задачу в очередь.
     *
     * @param task задача для добавления
     * @return {@code true} если задача была добавлена
     */
    public boolean addTask(MapTask task) {
        return mapTasks.add(task);
    }

    /**
     * Взять задачу из очереди.
     * Может вернуть {@code null}.
     *
     * @return задача из очереди
     */
    public MapTask pollTask() {
        return mapTasks.poll();
    }
}
