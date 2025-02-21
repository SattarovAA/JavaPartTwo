package org.jpt.util;

import lombok.extern.slf4j.Slf4j;
import org.jpt.demo.impl.MapReduceDemo;
import org.jpt.model.KeyValue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class FileUtil {
    /**
     * Базовая директория для работы.
     */
    private static final String WORK_DIRECTORY_WITH_SEPARATOR = "data" + File.separator;
    /**
     * Директория для чтения Input файлов.
     */
    private static final String INPUT_DIRECTORY = WORK_DIRECTORY_WITH_SEPARATOR + "input";
    /**
     * Директория для записи и чтения Intermediate файлов.
     */
    private static final String INTERMEDIATE_DIRECTORY = WORK_DIRECTORY_WITH_SEPARATOR + "intermediate";
    /**
     * Директория для записи Output файлов.
     */
    private static final String OUTPUT_DIRECTORY = WORK_DIRECTORY_WITH_SEPARATOR + "output";
    /**
     * Строка с которой начинается имя Intermediate файла.
     */
    private static final String INTERMEDIATE_STARTS_WITH = "mr-";
    /**
     * Строка с которой начинается имя Output файла.
     */
    private static final String OUTPUT_STARTS_WITH = "output-";
    /**
     * Разделитель в Intermediate файле.
     */
    private static final String MR_SEPARATOR = "-";
    /**
     * Формат output файла.
     */
    private static final String OUTPUT_FORMAT = ".txt";
    /**
     * Regex для проверки соответствия найденного имени файла
     * шаблону имени Intermediate файла.
     */
    private static final String INTERMEDIATE_FILENAME_REGEX = "mr-\\d+-\\d+";
    /**
     * Пустой массив для возврата значения
     * в случае проблем с загрузкой файла из директории.
     *
     * @see #loadFiles(String)
     */
    private static final File[] EMPTY_FILE_ARRAY = {};

    /**
     * Очистка директорий для работы.
     *
     * @see #INTERMEDIATE_DIRECTORY
     * @see #OUTPUT_DIRECTORY
     */
    public static void clearWorkDirectories() {
        deleteDirectoryContents(new File(INTERMEDIATE_DIRECTORY));
        deleteDirectoryContents(new File(OUTPUT_DIRECTORY));
    }

    /**
     * Рекурсивно удаляет все файлы и директории в указанном файле.
     *
     * @param directory директория для очистки
     */
    private static void deleteDirectoryContents(File directory) {
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectoryContents(file);
                    }
                    file.delete();
                }
            }
        }
        log.warn("Directories {} were cleared.", directory.getAbsolutePath());
    }

    /**
     * Извлекает данные из файла с путем {@code path}.
     *
     * @param path путь к файлу с данными
     * @return текстовое содержание файла
     * @throws IOException if an I/O error occurs reading from the file
     */
    public static String getContent(String path) throws IOException {
        return Files.readString(Paths.get(path));
    }

    /**
     * Возвращает список файлов input директории.
     *
     * @return список файлов input директории
     */
    public static File[] loadInputFiles() {
        return loadFiles(INPUT_DIRECTORY);
    }

    /**
     * Возвращает список файлов intermediate директории.
     *
     * @return список файлов intermediate директории
     */
    public static File[] loadIntermediateFiles() {
        return loadFiles(INTERMEDIATE_DIRECTORY);
    }

    /**
     * Возвращает список файлов из директории с адресом {@code dirPath}.
     *
     * @return список файлов из директории по пути
     */
    private static File[] loadFiles(String dirPath) {
        File dir = new File(dirPath);

        if (!dir.exists() || !dir.isDirectory()) {
            log.error("Directory: {} not found!", dirPath);
            return EMPTY_FILE_ARRAY;
        }

        File[] files = dir.listFiles();

        if (files == null || files.length == 0) {
            log.error("Files in directory({}) were not found!", dirPath);
            return EMPTY_FILE_ARRAY;
        }
        return dir.listFiles();
    }

    /**
     * Преобразует {@code str} в int значение
     * в промежутке от 0 до количества reduce-задач
     * с помощью hash-функции.
     *
     * @param str строка для преобразования
     * @return int для reduce bucket-а
     */
    public static int getBucket(String str) {
        return Math.abs(str.hashCode()) % MapReduceDemo.getReduceAmount();
    }

    /**
     * Возвращает корректное имя intermediate файла
     * из id задачи и номера reduce-задачи.
     *
     * @param taskId id задачи
     * @param key    id reduce-задачи
     * @return Корректное имя intermediate файла
     */
    public static String getIntermediateFileName(int taskId, Integer key) {
        return INTERMEDIATE_STARTS_WITH + taskId + MR_SEPARATOR + key;
    }

    /**
     * Возвращает корректное имя output файла из id задачи.
     *
     * @param taskId id задачи
     * @return Корректное имя output файла
     */
    public static String getOutputFileName(int taskId) {
        return OUTPUT_STARTS_WITH + taskId + OUTPUT_FORMAT;
    }

    /**
     * Проверка имени файла на соответствие шаблону имени
     * intermediate файла.
     *
     * @param fileName имя файла
     * @return {@code true} если имя корректное
     */
    public static boolean checkMrFileName(String fileName) {
        return fileName.matches(INTERMEDIATE_FILENAME_REGEX);
    }

    /**
     * Записывает в intermediate файл данные из values.
     *
     * @param fileName имя файла
     * @param values   данные для записи
     */
    public static void writeIntermediateFile(String fileName, List<KeyValue> values) {
        checkWorkDir(INTERMEDIATE_DIRECTORY);

        String filePath = INTERMEDIATE_DIRECTORY + File.separator + fileName;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (KeyValue kv : values) {
                writer.write(kv.key() + " " + kv.value());
                writer.newLine();
            }
            log.info("Intermediate file was written: {}", filePath);
        } catch (IOException e) {
            log.error("Error writing intermediate file: {}", filePath, e);
        }
    }

    /**
     * Записывает в output файл данные из values.
     *
     * @param fileName имя файла
     * @param results  данные для записи
     */
    public static void writeOutputFile(String fileName, Map<String, String> results) {
        checkWorkDir(OUTPUT_DIRECTORY);

        String filePath = OUTPUT_DIRECTORY + File.separator + fileName;
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath))) {
            for (var entry : results.entrySet()) {
                writer.write(entry.getKey() + " " + entry.getValue());
                writer.newLine();
            }
            log.info("Output file was written: {}", filePath);
        } catch (IOException e) {
            log.error("Error writing output file: {}", filePath, e);
        }
    }

    /**
     * Проверяет наличие директории с адресом path.
     * Создает если директория по адресу отсутствует.
     *
     * @param path путь к директории
     */
    private static void checkWorkDir(String path) {
        File directory = new File(path);
        if (!directory.exists()) {
            log.info("Directory {} was not found. Creating...", path);
            directory.mkdirs();
        }
    }

    /**
     * Извлекает из файла с адресом {@code absolutePath}.
     * Преобразует строки в разделенные \\s
     * в список значение {@link KeyValue}.
     *
     * @param absolutePath полный путь к директории
     * @return список с данными файла
     */
    public static List<KeyValue> readIntermediateFile(String absolutePath) {
        List<KeyValue> keyValues = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(absolutePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length == 2) {
                    keyValues.add(new KeyValue(parts[0], parts[1]));
                }
            }
        } catch (IOException e) {
            log.error("Error reading from intermediate file: {}", absolutePath, e);
        }
        return keyValues;
    }
}
