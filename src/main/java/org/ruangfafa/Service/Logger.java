package org.ruangfafa.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    public static String log(String message, String workDic) {
        String log = message + " [" + workDic + "] ["+ LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) +"]";
        System.out.println(log);
        return log;
    }
}
