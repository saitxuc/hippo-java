package com.hippoconsoleweb.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;

/**
 * 
 * @author DPJ
 * 2014-09-26
 */
public class FastjsonUtil {

    public static String objToJson(Object obj) {
        return JSONObject.toJSONString(obj);
    }

    public static <T> T jsonToObj(String text, Class<T> clazz) {
        T t = null;
        try {
            t = JSON.parseObject(text, clazz);
        } catch (Exception e) {

        }
        return t;
    }
    
    public static <T> T jsonToObj(final String text, final TypeReference<T> typeReference) {
        T t = null;
        try {
            t = (T)JSON.parseObject(text, typeReference);
        } catch (Exception e) {
        	e.printStackTrace();
        }
        return t;
    }

}
