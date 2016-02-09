package com.razorthink.bigbrain.interpreters;

import java.util.Map;

/**
 * Interpreter context
 */
public class InterpreterContext {
    private static final ThreadLocal<InterpreterContext> threadIC =
            new ThreadLocal<InterpreterContext>();
    public final InterpreterOutput out;

    public static InterpreterContext get() {
        return threadIC.get();
    }

    public static void set(InterpreterContext ic) {
        threadIC.set(ic);
    }

    public static void remove() {
        threadIC.remove();
    }

    private final String uniqueId;

    private final Map<String, Object> config;


    public InterpreterContext(String uniqueId,
                              Map<String, Object> config,
                              InterpreterOutput out
    ) {
        this.uniqueId = uniqueId;
        this.config = config;
        this.out = out;
    }

    public Map<String, Object> getConfig() {
        return config;
    }
}
