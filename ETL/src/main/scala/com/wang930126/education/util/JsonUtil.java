package com.wang930126.education.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JsonUtil {
    public static JSONObject getJsonObject(String text){
        try{
            return JSON.parseObject(text);
        }catch(Exception e){
            return null;
        }
    }
}
