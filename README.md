# scala-spark-interpreter

just do mvn clean install
Run App.main --> for stand alone
Run WebInit --> for web based

Make sure spark is runing or it can run standlone by changing the master url inside,
```
SparkScalaInterpreter - replace below code with, 
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
    
```    
 
this one,
```
static {
    Interpreter.register(
        "spark",
        "spark",
        SparkInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add("spark.app.name", "Zeppelin", "The name of spark application.")
            .add("master",
                getSystemDefault("MASTER", "spark.master", "local[*]"),
                "Spark master uri. ex) spark://masterhost:7077")
            .add("spark.executor.memory",
                getSystemDefault(null, "spark.executor.memory", "512m"),
                "Executor memory per worker instance. ex) 512m, 32g")
            .add("spark.cores.max",
                getSystemDefault(null, "spark.cores.max", ""),
                "Total number of cores to use. Empty value uses all available core.")
            .add("zeppelin.spark.useHiveContext",
                getSystemDefault("ZEPPELIN_SPARK_USEHIVECONTEXT",
                    "zeppelin.spark.useHiveContext", "true"),
                "Use HiveContext instead of SQLContext if it is true.")
            .add("zeppelin.spark.maxResult",
                getSystemDefault("ZEPPELIN_SPARK_MAXRESULT", "zeppelin.spark.maxResult", "1000"),
                "Max number of SparkSQL result to display.")
            .add("args", "", "spark commandline args").build());

  }
```
