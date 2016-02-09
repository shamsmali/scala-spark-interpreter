package com.razorthink.bigbrain.interpreters;

/**
 * Created by shams on 2/8/16.
 */
public interface InterpreterOutputListener {
    /**
     * called when newline is detected
     * @param line
     */
    public void onAppend(InterpreterOutput out, byte[] line);

    /**
     * when entire output is updated. eg) after detecting new display system
     * @param output
     */
    public void onUpdate(InterpreterOutput out, byte[] output);
}
