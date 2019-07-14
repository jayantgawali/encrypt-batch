package com.wipro.batch.util;

import java.io.File;

public class Utils {

    public static final String OUTPUT_FILE_SUFFIX = "output";

    public static int convertToInt(String value, int defaultValue) {
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static String createTmpOutputFilePath(File file) {
        return file.getParentFile().getPath() + File.separator + OUTPUT_FILE_SUFFIX;
    }
}
