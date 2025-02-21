package org.jpt.util;

public class StringUtil {
    /**
     * Разделитель в Intermediate файле.
     */
    private static final String MR_SEPARATOR = "-";

    /**
     * Извлекает из имени Intermediate файла номер reduce задачи.
     *
     * @param fileName Имя Intermediate файла
     * @return Строка после последнего {@link #MR_SEPARATOR}
     */
    public static String substringAfterLast(String fileName) {
        return fileName.substring(fileName.lastIndexOf(MR_SEPARATOR) + 1);
    }
}
