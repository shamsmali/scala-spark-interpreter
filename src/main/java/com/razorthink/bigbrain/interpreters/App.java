package com.razorthink.bigbrain.interpreters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by shams on 2/8/16.
 */
public class App {
    public static void main(String[] args) {
        SparkScalaInterpreter sparkScalaInterpreter = new SparkScalaInterpreter(new Properties());
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

        InterpreterContext interpreterContext = new InterpreterContext(UUID.randomUUID().toString(), new HashMap<String, Object>(), interpreterOutput);
        sparkScalaInterpreter.open();

        InterpreterResult r =  sparkScalaInterpreter.interpret("case class message(name : String)", interpreterContext);

        sparkScalaInterpreter.interpret("val p = message(\"shams mali\")", interpreterContext);
        sparkScalaInterpreter.interpret("p.name", interpreterContext);
        sparkScalaInterpreter.interpret("def toUpper(x : String) : String = x.toUpperCase", interpreterContext);
        sparkScalaInterpreter.interpret("toUpper(p.name)", interpreterContext);
        sparkScalaInterpreter.interpret("def square(x : Int) : Int = x * x; square(10)", interpreterContext);


        sparkScalaInterpreter.interpret("sc.toString()", interpreterContext);
        sparkScalaInterpreter.interpret("case class Person(name:String, age:Int)\n", interpreterContext);
        sparkScalaInterpreter.interpret("val people = sc.parallelize(Seq(Person(\"moon\", 33), Person(\"jobs\", 51), Person(\"gates\", 51), Person(\"park\", 34)))\n", interpreterContext);

        sparkScalaInterpreter.interpret("people.take(3)", interpreterContext);

        sparkScalaInterpreter.interpret("import scala.collection.mutable.ArrayBuffer",interpreterContext);

        sparkScalaInterpreter.interpret("val ab = ArrayBuffer(\"12\", \"21\")",interpreterContext);

        sparkScalaInterpreter.getSparkContext().stop();
    }

}
