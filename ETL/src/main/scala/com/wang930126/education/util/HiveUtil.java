package com.wang930126.education.util;

import org.apache.spark.sql.SparkSession;

public class HiveUtil {

    public static void openDynamicPartition(SparkSession spark){
        spark.sql("set hive.exec.dynamic.partition=true");
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
    }

    public static void openCompression(SparkSession spark){
        spark.sql("set mapred.output.compress=true");
        spark.sql("set hive.exec.output.compress=true");
    }

    public static void openSnappyCompression(SparkSession spark){
        spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
        spark.sql("set mapreduce.output.fileoutputformat.compress=true");
        spark.sql("mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
    }

}
