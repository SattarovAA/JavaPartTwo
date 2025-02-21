package org.jpt.thread;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jpt.controller.LatchController;
import org.jpt.controller.TaskQueueController;
import org.jpt.model.MapTask;
import org.jpt.model.ReduceTask;
import org.jpt.util.FileUtil;
import org.jpt.util.StringUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Класс для создания Map и Reduce задач.
 */
@RequiredArgsConstructor
@Slf4j
public class Coordinator implements Runnable {
    private final LatchController latchController;
    private final TaskQueueController queueController;
    private final int reduceAmount;
    private int taskCounter;

    @Override
    public void run() {
        log.info("Coordinator starts...");

        log.info("Coordinator started Map Phase.");
        mapPhaseWork();

        log.info("Coordinator Map Phase end.");
        latchController.awaitMapPhaseEnd();

        log.info("Coordinator started Reduce Phase.");
        reducePhaseWork();

        log.info("Coordinator ends...");
    }

    /**
     * Map-фаза работы координатора.
     * Получает input файлы.
     * Преобразует данные в {@link MapTask}
     * и отправляет их в работу через {@link #queueController}.
     * <p>
     * Отправляет полученное кол-во задач в {@link #latchController}
     * для инициации {@code MapPhaseLatch}.
     * Инициирует начало Map-фазы для Worker-ов.
     */
    private void mapPhaseWork() {
        File[] inputFiles = FileUtil.loadInputFiles();
        latchController.initMapPhaseLatch(inputFiles.length);
        sendMapTasks(inputFiles);
        latchController.startMapPhaseCall();
    }

    /**
     * Преобразует список файлов в список {@link MapTask}
     * с аналогичным размером
     * и отправляет в {@link #queueController}.
     *
     * @param files файлы для преобразования в задачи
     */
    private void sendMapTasks(File[] files) {
        Arrays.stream(files)
                .forEach(f -> queueController.addMapTask(
                        new MapTask(taskCounter++, f.getAbsolutePath())
                ));
    }

    /**
     * Reduce-фаза работы координатора.
     * Получает intermediate файлы.
     * В соответствие с именем файла преобразует множество файлов
     * в {@link #reduceAmount} {@link ReduceTask}.
     * Отправляет их в работу через {@link #queueController}.
     * <p>
     * Отправляет полученное кол-во задач в {@link #latchController}
     * для инициации {@code MapPhaseLatch}.
     * Инициирует начало Map-фазы для Worker-ов.
     */
    private void reducePhaseWork() {
        File[] intermediateFiles = FileUtil.loadIntermediateFiles();
        Map<String, List<String>> taskMap = getReduceTaskMap();
        fillReduceTaskMap(taskMap, intermediateFiles);
        sendReduceTasks(taskMap);
        latchController.startReducePhaseCall();
    }

    /**
     * Создает словарь {@link #reduceAmount} размера
     * key : итеративное значение.
     * value : пустой {@link ArrayList}.
     *
     * @return Map для распределения данных для reduce задачи
     */
    private Map<String, List<String>> getReduceTaskMap() {
        Map<String, List<String>> reduceFiles = new HashMap<>();
        for (int i = 0; i < reduceAmount; i++) {
            reduceFiles.put(String.valueOf(i), new ArrayList<>());
        }
        return reduceFiles;
    }

    /**
     * Проверяет файлы на корректность имени.
     * Заполняет {@code taskMap} значения из {@code files}
     * в соответствии с их именем и количеством reduce-задач.
     *
     * @param taskMap map с распределенными для reduce-задач данными
     * @param files   список файлов для заполнения map
     */
    private void fillReduceTaskMap(Map<String, List<String>> taskMap, File[] files) {
        Arrays.stream(files)
                .filter(file -> FileUtil.checkMrFileName(file.getName()))
                .forEach(f -> addToReduceTaskMap(taskMap, f));
    }

    /**
     * Добавляет абсолютный путь {@code file} в {@code taskMap}
     * в соответствии с номером reduce-задачи
     * полученной из имени {@code file}.
     *
     * @param taskMap map для добавления нового элемента
     * @param file    файл для добавления в map
     */
    private void addToReduceTaskMap(Map<String, List<String>> taskMap, File file) {
        taskMap.get(StringUtil.substringAfterLast(file.getName()))
                .add(file.getAbsolutePath());
    }

    /**
     * Преобразует {@code taskMap} в список {@link ReduceTask}.
     * Отправляет в {@link #queueController}.
     *
     * @param taskMap map с распределенными для reduce-задач данными
     */
    private void sendReduceTasks(Map<String, List<String>> taskMap) {
        taskMap.values()
                .forEach(list -> queueController.addReduceTask(
                        new ReduceTask(taskCounter++, list)
                ));
    }
}
