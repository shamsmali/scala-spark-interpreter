package com.razorthink.bigbrain.interpreters;

import java.lang.reflect.InvocationTargetException;

/**
 * Interpreter utility functions
 */
public class InterpreterUtils {

    public static String getMostRelevantMessage(Exception ex) {
        if (ex instanceof InvocationTargetException) {
            Throwable cause = ((InvocationTargetException) ex).getCause();
            if (cause != null) {
                return cause.getMessage();
            }
        }
        return ex.getMessage();
    }
}

