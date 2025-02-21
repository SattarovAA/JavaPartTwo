package org.jpt.thread;

import lombok.extern.slf4j.Slf4j;
import org.jpt.controller.LatchController;
import org.jpt.controller.TaskQueueController;
import org.jpt.model.KeyValue;
import org.jpt.model.MapTask;
import org.jpt.model.ReduceTask;
import org.jpt.util.FileUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Класс обработки Map и Reduce задач.
 */
@Slf4j
public class Worker implements Runnable {
    /**
     * Дефолтное значения для распределения данных в Map-фазе.
     *
     * @see #map(String)
     */
    private static final String DEFAULT_VALUE = "1";
    /**
     * Время ожидания в случае отсутствия задач.
     */
    private static final long SLEEPING_TIME = 50L;
    /**
     * Внутренний счетчик для инициации id worker-а.
     */
    private static final AtomicInteger workerCounter = new AtomicInteger();
    /**
     * Синхронизатор работы worker-ов по фазам.
     */
    private final LatchController latchController;
    /**
     * Раздатчик задач текущей фазы.
     */
    private final TaskQueueController queueController;
    /**
     * Индивидуальный номер работника для логирования.
     */
    private final int workerId;

    public Worker(LatchController latchController, TaskQueueController queueController) {
        this.latchController = latchController;
        this.queueController = queueController;
        this.workerId = workerCounter.incrementAndGet();
    }

    @Override
    public void run() {
        log.info("Worker {} starts...", workerId);
        latchController.awaitMapPhaseStart();

        log.info("Worker {} started Map Phase.", workerId);
        mapPhaseWork();
        latchController.awaitReducePhaseStart();

        log.info("Worker {} started Reduce Phase.", workerId);
        reducePhaseWork();

        log.info("Worker {} ends...", workerId);
    }

    /**
     * Map-фаза работы worker-а.
     * Работает с очередью через {@link #queueController}.
     * Заканчивает работу с завершением Map-фазы.
     */
    private void mapPhaseWork() {
        while (!latchController.isMapPhaseCompleted()) {
            MapTask task = queueController.pollMapTask();
            if (task == null) {
                awaitTask();
                continue;
            }
            acceptTask(task);
            latchController.mapTaskCompleteCall();
        }
    }

    /**
     * Ненадолго останавливает поток.
     * Используется для ожидания завершения работы другими потоками.
     *
     * @see #SLEEPING_TIME
     */
    private void awaitTask() {
        try {
            log.info("Worker {} waiting...", workerId);
            Thread.sleep(SLEEPING_TIME);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Обрабатывает задачу {@code task}
     * и записывает результат в файл.
     *
     * @param task задача для обработки
     */
    private void acceptTask(MapTask task) {
        try {
            String content = FileUtil.getContent(task.fileName());
            List<KeyValue> keyValues = map(content);
            Map<Integer, List<KeyValue>> bucketMap = getBucketMap(keyValues);
            bucketMap.forEach((bucket, values) ->
                    writeIntermediateFile(task.taskId(), bucket, values)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Преобразование текста в список {@link KeyValue}.
     * Key - слово из букв или цифр в тексте.
     * Value - "1"
     *
     * @param content текст файла со словами
     * @return список слов вместе с удельным весом одного слова
     */
    private List<KeyValue> map(String content) {
        String[] words = content.toLowerCase()
                .replaceAll("[^a-zа-яё\\d]", " ")
                .split("\\s+");

        return Arrays.stream(words)
                .map(w -> new KeyValue(w, DEFAULT_VALUE))
                .toList();
    }

    /**
     * Создает и наполняет значениями {@code keyValues}
     * словарь в соответствии с количеством reduce-задач.
     *
     * @param keyValues данные для распределения
     * @return Map в соответствии с количеством reduce-задач
     */
    private Map<Integer, List<KeyValue>> getBucketMap(List<KeyValue> keyValues) {
        Map<Integer, List<KeyValue>> bucketMap = new HashMap<>();
        for (KeyValue kv : keyValues) {
            int bucket = FileUtil.getBucket(kv.key());
            bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>())
                    .add(kv);
        }
        return bucketMap;
    }

    /**
     * Создание intermediate файла с {@code values}-содержимым
     * и корректным именем intermediate файла.
     *
     * @param taskId id map задачи для создания имени файла
     * @param key    id reduce задачи для создания имени файла
     * @param values список для передачи в файл
     */
    private void writeIntermediateFile(int taskId, Integer key, List<KeyValue> values) {
        String fileName = FileUtil.getIntermediateFileName(taskId, key);
        FileUtil.writeIntermediateFile(fileName, values);
    }

    /**
     * Reduce-фаза работы worker-а.
     * Работает с очередью через {@link #queueController}.
     * В случае если очередь пуста ненадолго засыпает.
     * Заканчивает работу с завершением Reduce-фазы.
     */
    private void reducePhaseWork() {
        while (!latchController.isReducePhaseCompleted()) {
            ReduceTask task = queueController.pollReduceTask();
            if (task == null) {
                awaitTask();
                continue;
            }
            acceptTask(task);
            latchController.reduceTaskCompleteCall();
        }
    }

    /**
     * Обрабатывает задачу {@code task}
     * и записывает результат в файл.
     * Преобразует несколько intermediate файлов
     * в один группируя их.
     * Key : конкретное слово.
     * Value : количество повторений слова в файлах.
     * Сортирует результат по алфавиту.
     *
     * @param task задача для обработки
     */
    private void acceptTask(ReduceTask task) {
        List<KeyValue> values = readIntermediateFiles(task.fileNames());
        Map<String, String> sortedData = new TreeMap<>();
        values.stream()
                .collect(Collectors.groupingBy(
                        KeyValue::key,
                        Collectors.counting()
                ))
                .forEach((k, v) -> sortedData.put(k, String.valueOf(v)));

        String outputFileName = FileUtil.getOutputFileName(task.taskId());
        FileUtil.writeOutputFile(outputFileName, sortedData);
    }

    /**
     * Преобразует список путей к файлам в
     * список {@link KeyValue} для дальнейшей обработки.
     *
     * @param fileNames список абсолютных путей.
     * @return список слово:значение из списка файлов
     */
    private List<KeyValue> readIntermediateFiles(List<String> fileNames) {
        List<KeyValue> values = new ArrayList<>();
        for (String file : fileNames) {
            values.addAll(FileUtil.readIntermediateFile(file));
        }
        return values;
    }
}
