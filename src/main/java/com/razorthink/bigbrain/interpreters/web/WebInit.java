package com.razorthink.bigbrain.interpreters.web;

/**
 * Created by shams on 2/8/16.
 */

import com.razorthink.bigbrain.interpreters.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import static spark.Spark.get;
import static spark.Spark.post;

public class WebInit {
    public static void main(String[] args) {

        get("/hello", (req, res) -> "Hello World");
        post("execute-scala", (req, res) -> {
            Interpreter sparkScalaInterpreter = getSparkInterpreter();
            sparkScalaInterpreter.interpret(req.body(), getContext());
            getContext().out.flush();
            return new MyMessage(new String(getContext().out.toByteArray()));
        }, new JsonTransformer());
    }

    private static Interpreter getSparkInterpreter() {
        Interpreter sparkScalaInterpreter;
        if (null != InterPreterFactory.getInterpreters().get(SparkScalaInterpreter.class.getCanonicalName())) {
            sparkScalaInterpreter = InterPreterFactory.getInterpreters().get(SparkScalaInterpreter.class.getCanonicalName());
        } else {
            sparkScalaInterpreter = new SparkScalaInterpreter(new Properties());
            sparkScalaInterpreter.open();
            InterPreterFactory.getInterpreters().put(SparkScalaInterpreter.class.getCanonicalName(), sparkScalaInterpreter);
        }
        return sparkScalaInterpreter;
    }

    private static InterpreterContext getContext() {
        InterpreterContext interpreterContext;
        if (InterpreterContext.get() != null) {
            interpreterContext = InterpreterContext.get();
        } else {
            InterpreterOutput interpreterOutput = new InterpreterOutput(new InterpreterOutputListener() {
                @Override
                public void onAppend(InterpreterOutput out, byte[] line) {
                    System.out.println(new String(line));
                }

                @Override
                public void onUpdate(InterpreterOutput out, byte[] output) {
                    System.out.println(new String(output));
                }
            });
            interpreterContext = new InterpreterContext(UUID.randomUUID().toString(), new HashMap<String, Object>(), interpreterOutput);
            InterpreterContext.set(interpreterContext);
        }
        return interpreterContext;
    }
}
