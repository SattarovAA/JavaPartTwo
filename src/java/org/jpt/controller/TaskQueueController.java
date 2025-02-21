package org.jpt.controller;

import lombok.extern.slf4j.Slf4j;
import org.jpt.model.MapTask;
import org.jpt.model.ReduceTask;
import org.jpt.queue.MapTaskQueue;
import org.jpt.queue.ReduceTaskQueue;

/**
 * Класс для работы с очередями {@link MapTask} и {@link ReduceTask} задач.
 */
@Slf4j
public class TaskQueueController {
    /**
     * Очередь для работы с {@link MapTask} задачами.
     */
    private final MapTaskQueue mapTaskQueue;
    /**
     * Очередь для работы с {@link ReduceTask} задачами.
     */
    private final ReduceTaskQueue reduceTaskQueue;

    public TaskQueueController() {
        this.mapTaskQueue = new MapTaskQueue();
        this.reduceTaskQueue = new ReduceTaskQueue();
    }

    /**
     * Добавить {@link MapTask} задачу в очередь.
     *
     * @param task задача для добавления
     */
    public void addMapTask(MapTask task) {
        log.info("add task(id, fileName):{},{}",
                task.taskId(),
                task.fileName()
        );
        mapTaskQueue.addTask(task);
    }

    /**
     * Взять задачу из очереди.
     * Может вернуть {@code null}.
     *
     * @return задача из очереди
     */
    public MapTask pollMapTask() {
        MapTask task = mapTaskQueue.pollTask();
        if (task != null) {
            log.info("poll task(id, fileName):{},{}",
                    task.taskId(),
                    task.fileName()
            );
        }
        return task;
    }

    /**
     * Добавить {@link ReduceTask} задачу в очередь.
     *
     * @param task задача для добавления
     */
    public void addReduceTask(ReduceTask task) {
        log.info("add task(id, fileNames.size):{},{}",
                task.taskId(),
                task.fileNames().size()
        );
        reduceTaskQueue.offerTask(task);
    }

    /**
     * Взять задачу из очереди.
     * Может вернуть {@code null}.
     *
     * @return задача из очереди
     */
    public ReduceTask pollReduceTask() {
        ReduceTask task = reduceTaskQueue.pollTask();
        if (task != null) {
            log.info("poll task(id, fileNames.size):{},{}",
                    task.taskId(),
                    task.fileNames().size()
            );
        }
        return task;
    }
}
