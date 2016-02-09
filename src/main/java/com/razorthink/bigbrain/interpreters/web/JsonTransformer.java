package com.razorthink.bigbrain.interpreters.web;

import com.google.gson.Gson;
import spark.ResponseTransformer;

/**
 * Created by shams on 2/8/16.
 */
public class JsonTransformer implements ResponseTransformer {

    private Gson gson = new Gson();

    @Override
    public String render(Object model) {
        return gson.toJson(model);
    }

}