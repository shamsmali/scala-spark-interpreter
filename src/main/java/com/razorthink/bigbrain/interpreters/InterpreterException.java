package com.razorthink.bigbrain.interpreters;

/**
 * Created by shams on 2/8/16.
 */
public class InterpreterException extends RuntimeException {

    public InterpreterException(Throwable e) {
        super(e);
    }

    public InterpreterException(String m) {
        super(m);
    }

}