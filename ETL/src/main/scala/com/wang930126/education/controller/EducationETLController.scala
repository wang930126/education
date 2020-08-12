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

        //EducationETLService.etl4MemberRegtypeLog(sparkSession.sparkContext,sparkSession)
        //EducationETLService.elt4MemberLog(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4BaseAdLog(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4BaseWebSiteLog(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4MemPayMoneyLog(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4MemVipLevelLog(sparkSession.sparkContext,sparkSession)

        //EducationETLService.etl4QzChapter(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzChapterList(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzPoint(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzPointQuestion(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzSiteCourse(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzCourse(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzCourseEdusubject(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzWebsite(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzMajor(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzBusiness(sparkSession.sparkContext,sparkSession)
        //EducationETLService.etl4QzPaperView(sparkSession.sparkContext,sparkSession)
//        EducationETLService.etl4QzCenterPaper(sparkSession.sparkContext,sparkSession)
//        EducationETLService.etl4QzQuestionType(sparkSession.sparkContext,sparkSession)
//        EducationETLService.etl4QzQuestion(sparkSession.sparkContext,sparkSession)
//        EducationETLService.etl4QzCenter(sparkSession.sparkContext,sparkSession)
//        EducationETLService.etl4QzPaper(sparkSession.sparkContext,sparkSession)
//        EducationETLService.etl4QzMemberPaperQuestion(sparkSession.sparkContext,sparkSession)


    }
}
