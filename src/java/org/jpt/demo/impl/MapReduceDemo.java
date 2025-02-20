package org.jpt.demo.impl;

import lombok.extern.slf4j.Slf4j;
import org.jpt.controller.LatchController;
import org.jpt.controller.TaskQueueController;
import org.jpt.demo.Demo;
import org.jpt.thread.Coordinator;
import org.jpt.thread.Worker;
import org.jpt.util.FileUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class MapReduceDemo implements Demo {
    private static final int WORKER_AMOUNT = 4;
    private static final int REDUCE_AMOUNT = 5;
    private static final long SLEEPING_TIME = 100L;

    @Override
    public void run() {
        log.info("Demo starts.");
        FileUtil.clearWorkDirectories();

        LatchController latchController = new LatchController(REDUCE_AMOUNT);
        TaskQueueController queueController = new TaskQueueController();
        ExecutorService pool = null;
        try {
            pool = Executors.newFixedThreadPool(WORKER_AMOUNT);

            Thread masterThread = new Thread(
                    new Coordinator(latchController, queueController, REDUCE_AMOUNT)
            );

            for (int i = 0; i < WORKER_AMOUNT; i++) {
                pool.execute(new Worker(latchController, queueController));
            }
            masterThread.start();
        } finally {
            if (pool != null) {
                pool.shutdown();
            }
        }

        while (!pool.isTerminated()) {
            try {
                Thread.sleep(SLEEPING_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        log.info("Demo ends.");
    }

    /**
     * Для единственной функции в {@link FileUtil}.
     * Исключительно из-за желания не делать неудобные Property.
     *
     * @return reduce value
     */
    public static int getReduceAmount() {
        return REDUCE_AMOUNT;
    }
}
