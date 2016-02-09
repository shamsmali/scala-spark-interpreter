package com.razorthink.bigbrain.interpreters;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;

/**
 * Interface for interpreters.
 * If you want to implement new Zeppelin interpreter, extend this class
 *
 * Please see,
 * http://zeppelin.incubator.apache.org/docs/development/writingzeppelininterpreter.html
 *
 * open(), close(), interpreter() is three the most important method you need to implement.
 * cancel(), getProgress(), completion() is good to have
 * getFormType(), getScheduler() determine Zeppelin's behavior
 *
 */
public abstract class Interpreter {

    /**
     * Opens interpreter. You may want to place your initialize routine here.
     * open() is called only once
     */
    public abstract void open();

    /**
     * Closes interpreter. You may want to free your resources up here.
     * close() is called only once
     */
    public abstract void close();

    /**
     * Run code and return result, in synchronous way.
     *
     * @param st statements to run
     * @param context
     * @return
     */
    public abstract InterpreterResult interpret(String st, InterpreterContext context);

    /**
     * Optionally implement the canceling routine to abort interpret() method
     *
     * @param context
     */
    public abstract void cancel(InterpreterContext context);

    /**
     * Dynamic form handling
     * see http://zeppelin.incubator.apache.org/docs/dynamicform.html
     *
     * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}),
     *         FormType.NATIVE handles form in API
     */
    public abstract FormType getFormType();

    /**
     * get interpret() method running process in percentage.
     *
     * @param context
     * @return number between 0-100
     */
    public abstract int getProgress(InterpreterContext context);


    public abstract List<String> completion(String buf, int cursor);


    public static Logger logger = LoggerFactory.getLogger(Interpreter.class);

    private URL[] classloaderUrls;
    protected Properties property;

    public Interpreter(Properties property) {
        this.property = property;
    }

    public void setProperty(Properties property) {
        this.property = property;
    }

    public Properties getProperty() {
        Properties p = new Properties();
        p.putAll(property);

        Map<String, InterpreterProperty> defaultProperties = Interpreter
                .findRegisteredInterpreterByClassName(getClassName()).getProperties();
        for (String k : defaultProperties.keySet()) {
            if (!p.containsKey(k)) {
                String value = defaultProperties.get(k).getDefaultValue();
                if (value != null) {
                    p.put(k, defaultProperties.get(k).getDefaultValue());
                }
            }
        }

        return p;
    }

    public String getProperty(String key) {
        if (property.containsKey(key)) {
            return property.getProperty(key);
        }

        Map<String, InterpreterProperty> defaultProperties = Interpreter
                .findRegisteredInterpreterByClassName(getClassName()).getProperties();
        if (defaultProperties.containsKey(key)) {
            return defaultProperties.get(key).getDefaultValue();
        }

        return null;
    }


    public String getClassName() {
        return this.getClass().getName();
    }

    public URL[] getClassloaderUrls() {
        return classloaderUrls;
    }

    public void setClassloaderUrls(URL[] classloaderUrls) {
        this.classloaderUrls = classloaderUrls;
    }


    /**
     * Type of interpreter.
     */
    public static enum FormType {
        NATIVE, SIMPLE, NONE
    }

    /**
     * Represent registered interpreter class
     */
    public static class RegisteredInterpreter {
        private String name;
        private String group;
        private String className;
        private Map<String, InterpreterProperty> properties;
        private String path;

        public RegisteredInterpreter(String name, String group, String className,
                                     Map<String, InterpreterProperty> properties) {
            super();
            this.name = name;
            this.group = group;
            this.className = className;
            this.properties = properties;
        }

        public String getName() {
            return name;
        }

        public String getGroup() {
            return group;
        }

        public String getClassName() {
            return className;
        }

        public Map<String, InterpreterProperty> getProperties() {
            return properties;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }

    }

    /**
     * Type of Scheduling.
     */
    public static enum SchedulingMode {
        FIFO, PARALLEL
    }

    public static Map<String, RegisteredInterpreter> registeredInterpreters = Collections
            .synchronizedMap(new HashMap<String, RegisteredInterpreter>());

    public static void register(String name, String className) {
        register(name, name, className);
    }

    public static void register(String name, String group, String className) {
        register(name, group, className, new HashMap<String, InterpreterProperty>());
    }

    public static void register(String name, String group, String className,
                                Map<String, InterpreterProperty> properties) {
        registeredInterpreters.put(group + "." + name, new RegisteredInterpreter(
                name, group, className, properties));
    }

    public static RegisteredInterpreter findRegisteredInterpreterByClassName(String className) {
        for (RegisteredInterpreter ri : registeredInterpreters.values()) {
            if (ri.getClassName().equals(className)) {
                return ri;
            }
        }
        return null;
    }
}
