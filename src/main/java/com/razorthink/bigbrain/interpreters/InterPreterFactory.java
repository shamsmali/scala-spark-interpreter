package com.razorthink.bigbrain.interpreters;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by shams on 2/8/16.
 */
public class InterPreterFactory {
    public static final InterPreterFactory INTER_PRETER_FACTORY = new InterPreterFactory();
    private static Map<String, Interpreter> interpreters = new HashMap<>();

    public static Map<String, Interpreter> getInterpreters() {
        return interpreters;
    }

    public static void setInterpreters(Map<String, Interpreter> interpreters) {
        InterPreterFactory.interpreters = interpreters;
    }
}
