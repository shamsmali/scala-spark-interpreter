package com.razorthink.bigbrain.interpreters.web;

/**
 * Created by shams on 2/8/16.
 */
public class MyMessage {
    private String message;

    public MyMessage(String s) {
        this.message = s;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
