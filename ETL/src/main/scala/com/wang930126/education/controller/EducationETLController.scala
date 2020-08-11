package com.wang930126.education.controller

import com.wang930126.education.etl.EducationETLService
import com.wang930126.education.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EducationETLController {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
        val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        // set hive.exec.dynamic.partition=true
        // set hive.exec.dynamic.partition.mode=nonstrict
        HiveUtil.openDynamicPartition(sparkSession)

        // set mapred.output.compress=true
        // set hive.exec.compress.output=true
        // HiveUtil.openCompression(sparkSession)

        /*
        set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
        set mapreduce.output.fileoutputformat.compress=true
        set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
        */
        // HiveUtil.openSnappyCompression(sparkSession)

        EducationETLService.etl4MemberRegtypeLog(sparkSession.sparkContext,sparkSession)
        EducationETLService.elt4MemberLog(sparkSession.sparkContext,sparkSession)
        EducationETLService.etl4BaseAdLog(sparkSession.sparkContext,sparkSession)
        EducationETLService.etl4BaseWebSiteLog(sparkSession.sparkContext,sparkSession)
        EducationETLService.etl4MemPayMoneyLog(sparkSession.sparkContext,sparkSession)
        EducationETLService.etl4MemVipLevelLog(sparkSession.sparkContext,sparkSession)
    }
}
