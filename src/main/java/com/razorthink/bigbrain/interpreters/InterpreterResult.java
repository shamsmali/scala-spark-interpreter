package com.razorthink.bigbrain.interpreters;

/**
 * Created by shams on 2/8/16.
 */

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Interpreter result template.
 */
public class InterpreterResult implements Serializable {

    /**
     *  Type of result after code execution.
     */
    public static enum Code {
        SUCCESS,
        INCOMPLETE,
        ERROR,
        KEEP_PREVIOUS_RESULT
    }

    /**
     * Type of Data.
     */
    public static enum Type {
        TEXT,
        HTML,
        ANGULAR,
        TABLE,
        IMG,
        SVG,
        NULL
    }

    Code code;
    Type type;
    String msg;

    public InterpreterResult(Code code) {
        this.code = code;
        this.msg = null;
        this.type = Type.TEXT;
    }

    public InterpreterResult(Code code, String msg) {
        this.code = code;
        this.msg = getData(msg);
        this.type = getType(msg);
    }

    public InterpreterResult(Code code, Type type, String msg) {
        this.code = code;
        this.msg = msg;
        this.type = type;
    }

    /**
     * Magic is like %html %text.
     *
     * @param msg
     * @return
     */
    private String getData(String msg) {
        if (msg == null) {
            return null;
        }
        Type[] types = type.values();
        TreeMap<Integer, Type> typesLastIndexInMsg = buildIndexMap(msg);
        if (typesLastIndexInMsg.size() == 0) {
            return msg;
        } else {
            Map.Entry<Integer, Type> lastType = typesLastIndexInMsg.firstEntry();
            //add 1 for the % char
            int magicLength = lastType.getValue().name().length() + 1;
            // 1 for the last \n or space after magic
            int subStringPos = magicLength + lastType.getKey() + 1;
            return msg.substring(subStringPos);
        }
    }

    private Type getType(String msg) {
        if (msg == null) {
            return Type.TEXT;
        }
        Type[] types = type.values();
        TreeMap<Integer, Type> typesLastIndexInMsg = buildIndexMap(msg);
        if (typesLastIndexInMsg.size() == 0) {
            return Type.TEXT;
        } else {
            Map.Entry<Integer, Type> lastType = typesLastIndexInMsg.firstEntry();
            return lastType.getValue();
        }
    }

    private int getIndexOfType(String msg, Type t) {
        if (msg == null) {
            return 0;
        }
        String typeString = "%" + t.name().toLowerCase();
        return StringUtils.indexOf(msg, typeString );
    }

    private TreeMap<Integer, Type> buildIndexMap(String msg) {
        int lastIndexOftypes = 0;
        TreeMap<Integer, Type> typesLastIndexInMsg = new TreeMap<Integer, Type>();
        Type[] types = Type.values();
        for (Type t : types) {
            lastIndexOftypes = getIndexOfType(msg, t);
            if (lastIndexOftypes >= 0) {
                typesLastIndexInMsg.put(lastIndexOftypes, t);
            }
        }
        return typesLastIndexInMsg;
    }

    public Code code() {
        return code;
    }

    public String message() {
        return msg;
    }

    public Type type() {
        return type;
    }

    public InterpreterResult type(Type type) {
        this.type = type;
        return this;
    }

    public String toString() {
        return "%" + type.name().toLowerCase() + " " + msg;
    }
}
