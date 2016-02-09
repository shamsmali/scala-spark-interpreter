package com.razorthink.bigbrain.interpreters;

import com.google.common.base.Joiner;
import com.razorthink.bigbrain.interpreters.InterpreterResult.Code;
import org.apache.spark.HttpServer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.repl.SparkCommandLine;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.repl.SparkIMain;
import org.apache.spark.scheduler.Pool;
import org.apache.spark.sql.SQLContext;
import scala.*;
import scala.Enumeration;
import scala.tools.nsc.Settings;
import scala.tools.nsc.settings.MutableSettings;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Created by shams on 2/8/16.
 */
public class SparkScalaInterpreter extends Interpreter {
    private SparkILoop interpreter;
    private SparkIMain intp;
    private SparkOutputStream out;
    private SQLContext sqlc;
    private SparkContext sc;
    private SparkEnv env;
    private Map<String, Object> binder;

    public SparkScalaInterpreter(Properties property) {
        super(property);
        out = new SparkOutputStream();
    }

    static {
        Interpreter.register(
                "spark",
                "spark",
                SparkScalaInterpreter.class.getName(),
                new InterpreterPropertyBuilder()
                        .add("spark.app.name", "BigBrain", "The name of spark application.")
                        .add("master",
                                getSystemDefault("MASTER", "spark.master", "spark://10.0.0.30:7077"),
                                "Spark master uri. ex) spark://10.0.0.30:7077")
                        .add("spark.executor.memory",
                                getSystemDefault(null, "spark.executor.memory", "512m"),
                                "Executor memory per worker instance. ex) 512m, 32g")
                        .add("spark.cores.max",
                                getSystemDefault(null, "spark.cores.max", ""),
                                "Total number of cores to use. Empty value uses all available core.")
                        .add("zeppelin.spark.maxResult",
                                getSystemDefault("ZEPPELIN_SPARK_MAXRESULT", "zeppelin.spark.maxResult", "1000"),
                                "Max number of SparkSQL result to display.")
                        .add("args", "", "spark commandline args").build());

    }

    @Override
    public void open() {

        URL[] urls = getClassloaderUrls();

        Settings settings = new Settings();
        if (getProperty("args") != null) {
            String[] argsArray = getProperty("args").split(" ");
            LinkedList<String> argList = new LinkedList<String>();
            for (String arg : argsArray) {
                argList.add(arg);
            }

            SparkCommandLine command =
                    new SparkCommandLine(scala.collection.JavaConversions.asScalaBuffer(
                            argList).toList());
            settings = command.settings();
        }

        // set classpath for scala compiler
        MutableSettings.PathSetting pathSettings = settings.classpath();
        String classpath = "";
        List<File> paths = currentClassPath();
        for (File f : paths) {
            if (classpath.length() > 0) {
                classpath += File.pathSeparator;
            }
            classpath += f.getAbsolutePath();
        }

        if (urls != null) {
            for (URL u : urls) {
                if (classpath.length() > 0) {
                    classpath += File.pathSeparator;
                }
                classpath += u.getFile();
            }
        }

        pathSettings.v_$eq(classpath);
        settings.scala$tools$nsc$settings$ScalaSettings$_setter_$classpath_$eq(pathSettings);


        // set classloader for scala compiler
        settings.explicitParentLoader_$eq(new Some<ClassLoader>(Thread.currentThread()
                .getContextClassLoader()));
        MutableSettings.BooleanSetting b = (MutableSettings.BooleanSetting) settings.usejavacp();
        b.v_$eq(true);
        settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);

        SparkOutputStream out = new SparkOutputStream();
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
        out.setInterpreterOutput(interpreterOutput);

        interpreter = new SparkILoop(null, new PrintWriter(out));

        interpreter.settings_$eq(settings);

        interpreter.createInterpreter();

        intp = interpreter.intp();
        intp.setContextClassLoader();
        intp.initializeSynchronous();

        sc = getSparkContext();
        if (sc.getPoolForName("fair").isEmpty()) {
            Enumeration.Value schedulingMode = org.apache.spark.scheduler.SchedulingMode.FAIR();
            int minimumShare = 0;
            int weight = 1;
            Pool pool = new Pool("fair", schedulingMode, minimumShare, weight);
            sc.taskScheduler().rootPool().addSchedulable(pool);
        }

        sqlc = getSQLContext();

        intp.interpret("@transient var _binder = new java.util.HashMap[String, Object]()");
        binder = (Map<String, Object>) getValue("_binder");
        binder.put("sc", sc);
        binder.put("sqlc", sqlc);

        intp.interpret("@transient val sc = "
                + "_binder.get(\"sc\").asInstanceOf[org.apache.spark.SparkContext]");
        intp.interpret("@transient val sqlc = "
                + "_binder.get(\"sqlc\").asInstanceOf[org.apache.spark.sql.SQLContext]");
        intp.interpret("@transient val sqlContext = "
                + "_binder.get(\"sqlc\").asInstanceOf[org.apache.spark.sql.SQLContext]");
        intp.interpret("import org.apache.spark.SparkContext._");


        intp.interpret("import sqlContext.implicits._");
        intp.interpret("import sqlContext.sql");
        intp.interpret("import org.apache.spark.sql.functions._");

        try {

            Method loadFiles = this.interpreter.getClass().getMethod(
                    "org$apache$spark$repl$SparkILoop$$loadFiles", Settings.class);
            loadFiles.invoke(this.interpreter, settings);

        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            throw new InterpreterException(e);
        }

    }

    @Override
    public void close() {

    }

    public Object getValue(String name) {
        Object ret = intp.valueOfTerm(name);
        if (ret instanceof None) {
            return null;
        } else if (ret instanceof Some) {
            return ((Some) ret).get();
        } else {
            return ret;
        }
    }

    @Override
    public InterpreterResult interpret(String line, InterpreterContext context) {
        if (line == null || line.trim().length() == 0) {
            return new InterpreterResult(InterpreterResult.Code.SUCCESS);
        }
        return interpret(line.split("\n"), context);
    }

    public InterpreterResult interpret(String[] lines, InterpreterContext context) {
        synchronized (this) {
            InterpreterResult r = interpretInput(lines, context);
            return r;
        }
    }

    public InterpreterResult interpretInput(String[] lines, InterpreterContext context) {

        // add print("") to make sure not finishing with comment
        // see https://github.com/NFLabs/zeppelin/issues/151
        String[] linesToRun = new String[lines.length + 1];
        for (int i = 0; i < lines.length; i++) {
            linesToRun[i] = lines[i];
        }
        linesToRun[lines.length] = "print(\"\")";

        Console.setOut(context.out);
        out.setInterpreterOutput(context.out);
        context.out.clear();
        InterpreterResult.Code r = null;
        String incomplete = "";

        for (int l = 0; l < linesToRun.length; l++) {
            String s = linesToRun[l];
            // check if next line starts with "." (but not ".." or "./") it is treated as an invocation
            if (l + 1 < linesToRun.length) {
                String nextLine = linesToRun[l + 1].trim();
                if (nextLine.startsWith(".") && !nextLine.startsWith("..") && !nextLine.startsWith("./")) {
                    incomplete += s + "\n";
                    continue;
                }
            }

            scala.tools.nsc.interpreter.Results.Result res = null;
            try {
                res = intp.interpret(incomplete + s);
            } catch (Exception e) {
                out.setInterpreterOutput(null);
                logger.info("Interpreter exception", e);
                return new InterpreterResult(Code.ERROR, InterpreterUtils.getMostRelevantMessage(e));
            }

            r = getResultCode(res);

            if (r == Code.ERROR) {
                out.setInterpreterOutput(null);
                return new InterpreterResult(r, "");
            } else if (r == Code.INCOMPLETE) {
                incomplete += s + "\n";
            } else {
                incomplete = "";
            }
        }

        if (r == Code.INCOMPLETE) {

            out.setInterpreterOutput(null);
            return new InterpreterResult(r, "Incomplete expression");
        } else {

            out.setInterpreterOutput(null);
            return new InterpreterResult(Code.SUCCESS);
        }
    }

    public SQLContext getSQLContext() {
        if (sqlc == null) {
            sqlc = new SQLContext(getSparkContext());
        }
        return sqlc;
    }

    public synchronized SparkContext getSparkContext() {
        if (sc == null) {
            sc = createSparkContext();
            env = SparkEnv.get();
        }
        return sc;
    }

    public SparkContext createSparkContext() {
        System.err.println("------ Create new SparkContext " + getProperty("master") + " -------");

        String execUri = System.getenv("SPARK_EXECUTOR_URI");
        String[] jars = SparkILoop.getAddedJars();

        String classServerUri = null;

        try { // in case of spark 1.1x, spark 1.2x
            Method classServer = interpreter.intp().getClass().getMethod("classServer");
            HttpServer httpServer = (HttpServer) classServer.invoke(interpreter.intp());
            classServerUri = httpServer.uri();
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            // continue
        }

        if (classServerUri == null) {
            try { // for spark 1.3x
                Method classServer = interpreter.intp().getClass().getMethod("classServerUri");
                classServerUri = (String) classServer.invoke(interpreter.intp());
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                    | IllegalArgumentException | InvocationTargetException e) {
                throw new InterpreterException(e);
            }
        }

        SparkConf conf =
                new SparkConf()
                        .setMaster(getProperty("master"))
                        .setAppName(getProperty("spark.app.name"))
                        .set("spark.repl.class.uri", classServerUri);

        if (jars.length > 0) {
            conf.setJars(jars);
        }

        if (execUri != null) {
            conf.set("spark.executor.uri", execUri);
        }
        if (System.getenv("SPARK_HOME") != null) {
            conf.setSparkHome(System.getenv("SPARK_HOME"));
        }
        conf.set("spark.scheduler.mode", "FAIR");

        Properties intpProperty = getProperty();

        for (Object k : intpProperty.keySet()) {
            String key = (String) k;
            String val = intpProperty.get(key).toString();
            if (!key.startsWith("spark.") || !val.trim().isEmpty()) {
                logger.debug(String.format("SparkConf: key = [%s], value = [%s]", key, val));
                conf.set(key, val);
            }
        }

        //TODO(jongyoul): Move these codes into PySparkInterpreter.java
        String pysparkBasePath = getSystemDefault("SPARK_HOME", null, null);
        File pysparkPath;
        if (null == pysparkBasePath) {
            pysparkBasePath = getSystemDefault("ZEPPELIN_HOME", "zeppelin.home", "../");
            pysparkPath = new File(pysparkBasePath,
                    "interpreter" + File.separator + "spark" + File.separator + "pyspark");
        } else {
            pysparkPath = new File(pysparkBasePath,
                    "python" + File.separator + "lib");
        }

        //Only one of py4j-0.9-src.zip and py4j-0.8.2.1-src.zip should exist
        String[] pythonLibs = new String[]{"pyspark.zip", "py4j-0.9-src.zip", "py4j-0.8.2.1-src.zip"};
        ArrayList<String> pythonLibUris = new ArrayList<>();
        for (String lib : pythonLibs) {
            File libFile = new File(pysparkPath, lib);
            if (libFile.exists()) {
                pythonLibUris.add(libFile.toURI().toString());
            }
        }
        pythonLibUris.trimToSize();
        if (pythonLibs.length == pythonLibUris.size()) {
            conf.set("spark.yarn.dist.files", Joiner.on(",").join(pythonLibUris));
            if (!useSparkSubmit()) {
                conf.set("spark.files", conf.get("spark.yarn.dist.files"));
            }
            conf.set("spark.submit.pyArchives", Joiner.on(":").join(pythonLibs));
        }

        // Distributes needed libraries to workers.
        if (getProperty("master").equals("yarn-client")) {
            conf.set("spark.yarn.isPython", "true");
        }

        SparkContext sparkContext = new SparkContext(conf);
        return sparkContext;
    }

    private boolean useSparkSubmit() {
        return null != System.getenv("SPARK_SUBMIT");
    }

    @Override
    public void cancel(InterpreterContext context) {

    }

    @Override
    public FormType getFormType() {
        return null;
    }

    @Override
    public int getProgress(InterpreterContext context) {
        return 0;
    }

    @Override
    public List<String> completion(String buf, int cursor) {
        return null;
    }

    public static String getSystemDefault(
            String envName,
            String propertyName,
            String defaultValue) {

        if (envName != null && !envName.isEmpty()) {
            String envValue = System.getenv().get(envName);
            if (envValue != null) {
                return envValue;
            }
        }

        if (propertyName != null && !propertyName.isEmpty()) {
            String propValue = System.getProperty(propertyName);
            if (propValue != null) {
                return propValue;
            }
        }
        return defaultValue;
    }

    private List<File> currentClassPath() {
        List<File> paths = classPath(Thread.currentThread().getContextClassLoader());
        String[] cps = System.getProperty("java.class.path").split(File.pathSeparator);
        if (cps != null) {
            for (String cp : cps) {
                paths.add(new File(cp));
            }
        }
        return paths;
    }

    private List<File> classPath(ClassLoader cl) {
        List<File> paths = new LinkedList<File>();
        if (cl == null) {
            return paths;
        }

        if (cl instanceof URLClassLoader) {
            URLClassLoader ucl = (URLClassLoader) cl;
            URL[] urls = ucl.getURLs();
            if (urls != null) {
                for (URL url : urls) {
                    paths.add(new File(url.getFile()));
                }
            }
        }
        return paths;
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

    private Code getResultCode(scala.tools.nsc.interpreter.Results.Result r) {
        if (r instanceof scala.tools.nsc.interpreter.Results.Success$) {
            return Code.SUCCESS;
        } else if (r instanceof scala.tools.nsc.interpreter.Results.Incomplete$) {
            return Code.INCOMPLETE;
        } else {
            return Code.ERROR;
        }
    }
}
