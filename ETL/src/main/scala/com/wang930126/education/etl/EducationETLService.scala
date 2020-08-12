package com.wang930126.education.etl

import com.alibaba.fastjson.JSONObject
import com.wang930126.education.bean.{DwdQzPaperView, DwdQzPoint, DwdQzQuestion}
import com.wang930126.education.util.JsonUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 里面的方法对ods层各种表进行ETL处理
  */
object EducationETLService {

    /**
      * 解析用户做题情况数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzMemberPaperQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzMemberPaperQuestion.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val userid = jsonObject.getIntValue("userid")
                val paperviewid = jsonObject.getIntValue("paperviewid")
                val chapterid = jsonObject.getIntValue("chapterid")
                val sitecourseid = jsonObject.getIntValue("sitecourseid")
                val questionid = jsonObject.getIntValue("questionid")
                val majorid = jsonObject.getIntValue("majorid")
                val useranswer = jsonObject.getString("useranswer")
                val istrue = jsonObject.getString("istrue")
                val lasttime = jsonObject.getString("lasttime")
                val opertype = jsonObject.getString("opertype")
                val paperid = jsonObject.getIntValue("paperid")
                val spendtime = jsonObject.getIntValue("spendtime")
                val score = BigDecimal.apply(jsonObject.getString("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
                val question_answer = jsonObject.getIntValue("question_answer")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype, paperid, spendtime, score,question_answer, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_member_paper_question")
    }

    def etl4QzPaper(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzPaper.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val paperid = jsonObject.getIntValue("paperid")
                val papercatid = jsonObject.getIntValue("papercatid")
                val courseid = jsonObject.getIntValue("courseid")
                val paperyear = jsonObject.getString("paperyear")
                val chapter = jsonObject.getString("chapter")
                val suitnum = jsonObject.getString("suitnum")
                val papername = jsonObject.getString("papername")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val craetetime = jsonObject.getString("createtime")
                val totalscore = BigDecimal.apply(jsonObject.getString("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
                val chapterid = jsonObject.getIntValue("chapterid")
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, craetetime, totalscore, chapterid,
                        chapterlistid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_paper")
    }

    def etl4QzCenter(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzCenter.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(parititons => {
            parititons.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val centerid = jsonObject.getIntValue("centerid")
                val centername = jsonObject.getString("centername")
                val centeryear = jsonObject.getString("centeryear")
                val centertype = jsonObject.getString("centertype")
                val openstatus = jsonObject.getString("openstatus")
                val centerparam = jsonObject.getString("centerparam")
                val description = jsonObject.getString("description")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val sequence = jsonObject.getString("sequence")
                val provideuser = jsonObject.getString("provideuser")
                val centerviewtype = jsonObject.getString("centerviewtype")
                val stage = jsonObject.getString("stage")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
                        sequence, provideuser, centerviewtype, stage, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_center")
    }

    def etl4QzQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzQuestion.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val questionid = jsonObject.getIntValue("questionid")
                val parentid = jsonObject.getIntValue("parentid")
                val questypeid = jsonObject.getIntValue("questypeid")
                val quesviewtype = jsonObject.getIntValue("quesviewtype")
                val content = jsonObject.getString("content")
                val answer = jsonObject.getString("answer")
                val analysis = jsonObject.getString("analysis")
                val limitminute = jsonObject.getString("limitminute")
                val score = BigDecimal.apply(jsonObject.getDoubleValue("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
                val splitscore = BigDecimal.apply(jsonObject.getDoubleValue("splitscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
                val status = jsonObject.getString("status")
                val optnum = jsonObject.getIntValue("optnum")
                val lecture = jsonObject.getString("lecture")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val modifystatus = jsonObject.getString("modifystatus")
                val attanswer = jsonObject.getString("attanswer")
                val questag = jsonObject.getString("questag")
                val vanalysisaddr = jsonObject.getString("vanalysisaddr")
                val difficulty = jsonObject.getString("difficulty")
                val quesskill = jsonObject.getString("quesskill")
                val vdeoaddr = jsonObject.getString("vdeoaddr")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                DwdQzQuestion(questionid, parentid, questypeid, quesviewtype, content, answer, analysis, limitminute, score, splitscore,
                    status, optnum, lecture, creator, createtime, modifystatus, attanswer, questag, vanalysisaddr, difficulty, quesskill,
                    vdeoaddr, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_question")
    }

    def etl4QzQuestionType(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzQuestionType.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val quesviewtype = jsonObject.getIntValue("quesviewtype")
                val viewtypename = jsonObject.getString("viewtypename")
                val questiontypeid = jsonObject.getIntValue("questypeid")
                val description = jsonObject.getString("description")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val papertypename = jsonObject.getString("papertypename")
                val sequence = jsonObject.getString("sequence")
                val remark = jsonObject.getString("remark")
                val splitscoretype = jsonObject.getString("splitscoretype")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (quesviewtype, viewtypename, questiontypeid, description, status, creator, createtime, papertypename, sequence,
                        remark, splitscoretype, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_question_type")
    }


    def etl4QzCenterPaper(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzCenterPaper.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val paperviewid = jsonObject.getIntValue("paperviewid")
                val centerid = jsonObject.getIntValue("centerid")
                val openstatus = jsonObject.getString("openstatus")
                val sequence = jsonObject.getString("sequence")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_center_paper")
    }


    def etl4QzPaperView(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzPaperView.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val paperviewid = jsonObject.getIntValue("paperviewid")
                val paperid = jsonObject.getIntValue("paperid")
                val paperviewname = jsonObject.getString("paperviewname")
                val paperparam = jsonObject.getString("paperparam")
                val openstatus = jsonObject.getString("openstatus")
                val explainurl = jsonObject.getString("explainurl")
                val iscontest = jsonObject.getString("iscontest")
                val contesttime = jsonObject.getString("contesttime")
                val conteststarttime = jsonObject.getString("conteststarttime")
                val contestendtime = jsonObject.getString("contestendtime")
                val contesttimelimit = jsonObject.getString("contesttimelimit")
                val dayiid = jsonObject.getIntValue("dayiid")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val paperviewcatid = jsonObject.getIntValue("paperviewcatid")
                val modifystatus = jsonObject.getString("modifystatus")
                val description = jsonObject.getString("description")
                val papertype = jsonObject.getString("papertype")
                val downurl = jsonObject.getString("downurl")
                val paperuse = jsonObject.getString("paperuse")
                val paperdifficult = jsonObject.getString("paperdifficult")
                val testreport = jsonObject.getString("testreport")
                val paperuseshow = jsonObject.getString("paperuseshow")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                DwdQzPaperView(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime,
                    conteststarttime, contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus,
                    description, papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_paper_view")
    }


    /**
      * 解析做题业务
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzBusiness(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzBusiness.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item);
                val businessid = jsonObject.getIntValue("businessid")
                val businessname = jsonObject.getString("businessname")
                val sequence = jsonObject.getString("sequence")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val siteid = jsonObject.getIntValue("siteid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_business")
    }


    /**
      * 解析主修数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzMajor(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzMajor.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val majorid = jsonObject.getIntValue("majorid")
                val businessid = jsonObject.getIntValue("businessid")
                val siteid = jsonObject.getIntValue("siteid")
                val majorname = jsonObject.getString("majorname")
                val shortname = jsonObject.getString("shortname")
                val status = jsonObject.getString("status")
                val sequence = jsonObject.getString("sequence")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val columm_sitetype = jsonObject.getString("columm_sitetype")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, columm_sitetype, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_major")
    }


    /**
      * 解析课程网站
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzWebsite(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzWebsite.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val siteid = jsonObject.getIntValue("siteid")
                val sitename = jsonObject.getString("sitename")
                val domain = jsonObject.getString("domain")
                val sequence = jsonObject.getString("sequence")
                val multicastserver = jsonObject.getString("multicastserver")
                val templateserver = jsonObject.getString("templateserver")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val multicastgateway = jsonObject.getString("multicastgateway")
                val multicastport = jsonObject.getString("multicastport")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
                        multicastgateway, multicastport, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_website")
    }


    /**
      * 解析课程辅导数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzCourseEdusubject(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzCourseEduSubject.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val courseeduid = jsonObject.getIntValue("courseeduid")
                val edusubjectid = jsonObject.getIntValue("edusubjectid")
                val courseid = jsonObject.getIntValue("courseid")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val majorid = jsonObject.getIntValue("majorid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_course_edusubject")
    }


    /**
      * 解析课程数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzCourse(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzCourse.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val courseid = jsonObject.getIntValue("courseid")
                val majorid = jsonObject.getIntValue("majorid")
                val coursename = jsonObject.getString("coursename")
                val coursechapter = jsonObject.getString("coursechapter")
                val sequence = jsonObject.getString("sequnece")
                val isadvc = jsonObject.getString("isadvc")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val status = jsonObject.getString("status")
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val pointlistid = jsonObject.getIntValue("pointlistid")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status
                        , chapterlistid, pointlistid, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_course")
    }


    /**
      * 解析网站课程
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzSiteCourse(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzSiteCourse.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val sitecourseid = jsonObject.getIntValue("sitecourseid")
                val siteid = jsonObject.getIntValue("siteid")
                val courseid = jsonObject.getIntValue("courseid")
                val sitecoursename = jsonObject.getString("sitecoursename")
                val coursechapter = jsonObject.getString("coursechapter")
                val sequence = jsonObject.getString("sequence")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val helppaperstatus = jsonObject.getString("helppaperstatus")
                val servertype = jsonObject.getString("servertype")
                val boardid = jsonObject.getIntValue("boardid")
                val showstatus = jsonObject.getString("showstatus")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator
                        , createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_site_course")
    }


    /**
      * 解析知识点下的题数据
      *
      * @param ssc
      * @param sparkSession
      * @return
      */
    def etl4QzPointQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzPointQuestion.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val pointid = jsonObject.getIntValue("pointid")
                val questionid = jsonObject.getIntValue("questionid")
                val questtype = jsonObject.getIntValue("questtype")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (pointid, questionid, questtype, creator, createtime, dt, dn)
            })
        }).toDF().write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_point_question")
    }


    /**
      * 解析做题数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzPoint(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzPoint.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val pointid = jsonObject.getIntValue("pointid")
                val courseid = jsonObject.getIntValue("courseid")
                val pointname = jsonObject.getString("pointname")
                val pointyear = jsonObject.getString("pointyear")
                val chapter = jsonObject.getString("chapter")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val status = jsonObject.getString("status")
                val modifystatus = jsonObject.getString("modifystatus")
                val excisenum = jsonObject.getIntValue("excisenum")
                val pointlistid = jsonObject.getIntValue("pointlistid")
                val chapterid = jsonObject.getIntValue("chapterid")
                val sequence = jsonObject.getString("sequence")
                val pointdescribe = jsonObject.getString("pointdescribe")
                val pointlevel = jsonObject.getString("pointlevel")
                val typeslist = jsonObject.getString("typelist")
                val score = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP) //保留1位小数 并四舍五入
                val thought = jsonObject.getString("thought")
                val remid = jsonObject.getString("remid")
                val pointnamelist = jsonObject.getString("pointnamelist")
                val typelistids = jsonObject.getString("typelistids")
                val pointlist = jsonObject.getString("pointlist")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                DwdQzPoint(pointid, courseid, pointname, pointyear, chapter, creator, createtime, status, modifystatus, excisenum, pointlistid,
                    chapterid, sequence, pointdescribe, pointlevel, typeslist, score, thought, remid, pointnamelist, typelistids,
                    pointlist, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_point")
    }


    /**
      * 解析章节列表数据
      *
      * @param ssc
      * @param sparkSession
      */
    def etl4QzChapterList(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("hdfs://spark105:9000/user/education/ods/QzChapterList.log").filter(item => {
            val obj = JsonUtil.getJsonObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions(partitions => {
            partitions.map(item => {
                val jsonObject = JsonUtil.getJsonObject(item)
                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                val chapterlistname = jsonObject.getString("chapterlistname")
                val courseid = jsonObject.getIntValue("courseid")
                val chapterallnum = jsonObject.getIntValue("chapterallnum")
                val sequence = jsonObject.getString("sequence")
                val status = jsonObject.getString("status")
                val creator = jsonObject.getString("creator")
                val createtime = jsonObject.getString("createtime")
                val dt = jsonObject.getString("dt")
                val dn = jsonObject.getString("dn")
                (chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
            })
        }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("edu_dwd.dwd_qz_chapter_list")
    }

    def etl4QzChapter(sc:SparkContext,spark: SparkSession) = {
        //{"chapterid":0,
        // "chapterlistid":0,
        // "chaptername":"chaptername0",
        // "chapternum":10,
        // "courseid":61,
        // "createtime":"2019-07-22 16:37:24",
        // "creator":"admin",
        // "dn":"webA",
        // "dt":"20190722",
        // "outchapterid":0,
        // "sequence":"-",
        // "showstatus":"-",
        // "status":"-"}
        import spark.implicits._
        val textRDD: RDD[String] = sc.textFile("hdfs://spark105:9000/user/education/ods/QzChapter.log")
        textRDD.filter(JsonUtil.getJsonObject(_) != null)
                .mapPartitions{
                    partIter => {
                        partIter.map{
                            line => {
                                val jsonObject: JSONObject = JsonUtil.getJsonObject(line)
                                val chapterid = jsonObject.getIntValue("chapterid")
                                val chapterlistid = jsonObject.getIntValue("chapterlistid")
                                val chaptername = jsonObject.getString("chaptername")
                                val sequence = jsonObject.getString("sequence")
                                val showstatus = jsonObject.getString("showstatus")
                                val creator = jsonObject.getString("creator")
                                val createtime = jsonObject.getString("createtime")
                                val courseid = jsonObject.getIntValue("courseid")
                                val chapternum = jsonObject.getIntValue("chapternum")
                                val outchapterid = jsonObject.getIntValue("outchapterid")
                                val dt = jsonObject.getString("dt")
                                val dn = jsonObject.getString("dn")
                                (chapterid, chapterlistid, chaptername, sequence, showstatus, creator, createtime,
                                        courseid, chapternum, outchapterid, dt, dn)
                            }
                        }
                    }
                }
                .toDF()
                .coalesce(1)
                .write
                .mode(SaveMode.Append)
                .insertInto("edu_dwd.dwd_qz_chapter")
    }

    def etl4MemVipLevelLog(sparkContext: SparkContext, sparkSession: SparkSession) = {
        /*{"discountval":"-",
            "dn":"webA",
            "end_time":"2018-12-14",
            "last_modify_time":"2018-12-14",
            "max_free":"-",
            "min_free":"-",
            "next_level":"-",
            "operator":"update",
            "start_time":"2015-10-27",
            "vip_id":"0",
            "vip_level":"普通会员"}*/
        /*CREATE TABLE IF NOT EXISTS dwd_vip_level(
                vip_id int,
                vip_level string,
                start_time timestamp,
                end_time timestamp,
                last_modify_time timestamp,
                max_free string,
                min_free string,
                next_level string,
                operator string,
                dn string
        );*/
        val lineRDD: RDD[String] = sparkContext.textFile("hdfs://spark105:9000/user/education/ods/pcenterMemViplevel.log")
        import sparkSession.implicits._
        lineRDD.filter(JsonUtil.getJsonObject(_) != null)
                .map{
                    line => {
                        val jsonObj: JSONObject = JsonUtil.getJsonObject(line)
                        val vip_id: Int = jsonObj.getIntValue("vip_id")
                        val vip_level: String = jsonObj.getString("vip_level")
                        val start_time: String = jsonObj.getString("start_time")
                        val end_time: String = jsonObj.getString("end_time")
                        val last_modify_time: String = jsonObj.getString("last_modify_time")
                        val max_free: String = jsonObj.getString("max_free")
                        val min_free: String = jsonObj.getString("min_free")
                        val next_level: String = jsonObj.getString("next_level")
                        val operator: String = jsonObj.getString("operator")
                        val dn: String = jsonObj.getString("dn")
                        (vip_id,vip_level,start_time,end_time,last_modify_time,max_free,min_free,next_level,operator,dn)
                    }
                }.toDF()
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .insertInto("edu_dwd.dwd_vip_level")
    }

    def etl4MemPayMoneyLog(sparkContext: SparkContext, sparkSession: SparkSession)  = {

        /*{"dn":"webA","dt":"20190722","paymoney":"340.87","siteid":"2","uid":"51178","vip_id":"2"}*/
        /*CREATE TABLE IF NOT EXISTS dwd_pcentermempaymoney(
                uid int,
                paymoney string,
                siteid int,
                vip_id int,
                dt string,
                dn string
        )*/
        val lineRDD: RDD[String] = sparkContext.textFile("hdfs://spark105:9000/user/education/ods/pcentermempaymoney.log")
        import sparkSession.implicits._
        lineRDD.filter(JsonUtil.getJsonObject(_) != null)
                .mapPartitions{
                    partIter => {
                        partIter.map{
                            line => {
                                val jsonObj: JSONObject = JsonUtil.getJsonObject(line)
                                val uid: Int = jsonObj.getIntValue("uid")
                                val paymoney: String = jsonObj.getString("paymoney")
                                val siteid: Int = jsonObj.getIntValue("siteid")
                                val vip_id: Int = jsonObj.getIntValue("vip_id")
                                val dt: String = jsonObj.getString("dt")
                                val dn: String = jsonObj.getString("dn")
                                (uid,paymoney,siteid,vip_id,dt,dn)
                            }
                        }
                    }
                }.toDF()
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .insertInto("edu_dwd.dwd_pcentermempaymoney")
    }

    def etl4BaseWebSiteLog(sc:SparkContext,spark:SparkSession) = {

//        "createtime": "2000-01-01",
//        "creator": "admin",
//        "delete": "0",
//        "dn": "webC",  //网站分区
//        "siteid": "2",  //网站id
//        "sitename": "114",  //网站名称
//        "siteurl": "www.114.com/webC"  //网站地址

        val lineRDD: RDD[String] = sc.textFile("hdfs://spark105:9000/user/education/ods/baswewebsite.log")
        import spark.implicits._
        lineRDD.filter(JsonUtil.getJsonObject(_) != null)
                .map{
                    lineText => {
                        val jsonObj: JSONObject = JsonUtil.getJsonObject(lineText)
                        val createtime: String = jsonObj.getString("createtime")
                        val delete: Int = jsonObj.getIntValue("delete")
                        val creator: String = jsonObj.getString("creator")
                        val dn: String = jsonObj.getString("dn")
                        val siteid: Int = jsonObj.getIntValue("siteid")
                        val sitename: String = jsonObj.getString("sitename")
                        val siteurl: String = jsonObj.getString("siteurl")
                        (siteid,sitename,siteurl,delete,createtime,creator,dn)
                    }
                }.toDF()
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .insertInto("edu_dwd.dwd_base_website")
    }

    def etl4BaseAdLog(sc:SparkContext,session:SparkSession) = {
        import session.implicits._
        val lineRDD: RDD[String] = sc.textFile("hdfs://spark105:9000/user/education/ods/baseadlog.log")
        lineRDD.filter(JsonUtil.getJsonObject(_) != null)
                .mapPartitions{
                    partitionIter => partitionIter.map{
                        jsonString => {
                            val jsonObj: JSONObject = JsonUtil.getJsonObject(jsonString)
                            val adid: Int = jsonObj.getIntValue("adid")
                            val adname: String = jsonObj.getString("adname")
                            val dn: String = jsonObj.getString("dn")
                            (adid,adname,dn)
                        }
                    }
                }
                .toDF()
                .coalesce(1)
                .write
                .mode("append")
                .insertInto("edu_dwd.dwd_base_ad")
    }

    def log2MemberTuple(iter: Iterator[String]) = {
        iter.map{
            jsonString => {
                val jsonObj: JSONObject = JsonUtil.getJsonObject(jsonString)
                val ad_id: Int = jsonObj.getIntValue("ad_id")
                val birthday: String = jsonObj.getString("birthday")
                val dt: String = jsonObj.getString("dt")
                val dn: String = jsonObj.getString("dn")
                val email: String = jsonObj.getString("email")
                val fullname: String = jsonObj.getString("fullname").substring(0,1) + "xx"
                val iconurl: String = jsonObj.getString("iconurl")
                val lastlogin: String = jsonObj.getString("lastlogin")
                val mailaddr: String = jsonObj.getString("mailaddr")
                val memberlevel: String = jsonObj.getString("memberlevel")
                val password: String = "******"
                val paymoney: String = jsonObj.getString("paymoney")
                val phone: String = jsonObj.getString("phone")
                val newphone: String = phone.substring(0,3) + "*****" + phone.substring(7,11)
                val qq: String = jsonObj.getString("qq")
                val register: String = jsonObj.getString("register")
                val regupdatetime: String = jsonObj.getString("regupdatetime")
                val uid: Int = jsonObj.getIntValue("uid")
                val unitname: String = jsonObj.getString("unitname")
                val userip: String = jsonObj.getString("userip")
                val zipcode: String = jsonObj.getString("zipcode")
                (uid,ad_id,birthday,email,fullname,iconurl,lastlogin,mailaddr,memberlevel,
                password,paymoney,phone,qq,register,regupdatetime,unitname,userip,zipcode,
                dt,dn)
            }
        }
    }

    def elt4MemberLog(sc:SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        val textRDD: RDD[String] = sc.textFile("hdfs://spark105:9000/user/education/ods/member.log")
        textRDD.filter(JsonUtil.getJsonObject(_) != null)
                .mapPartitions(iter => log2MemberTuple(iter))
                .toDF()
                .coalesce(2)
                .write
                .mode("append")
                .insertInto("edu_dwd.dwd_member")
    }

    /**
      * 对 MemberRegtypeLog的数据进行ETL
      * {"appkey":"-",
      * "appregurl":"http:www.webA.com/sale/register/index.html",
      * "bdp_uuid":"-",
      * "createtime":"2017-03-30",
      * "dn":"webA",
      * "domain":"-",
      * "dt":"20190722",
      * "isranreg":"-",
      * "regsource":"0",
      * "uid":"0",
      * "websiteid":"4"}
      */
    def etl4MemberRegtypeLog(sc:SparkContext,sparkSession:SparkSession) = {
        import sparkSession.implicits._
        val textRDD: RDD[String] = sc.textFile("hdfs://spark105:9000/user/education/ods/memberRegtype.log")
        textRDD.filter(JsonUtil.getJsonObject(_) != null)
                .mapPartitions(iter => log2MemberRegtypeTuple(iter))
                .toDF()
                .coalesce(1)
                .write.mode("append").insertInto("edu_dwd.dwd_member_regtype")
    }

    def log2MemberRegtypeTuple(iter:Iterator[String]) = {
        iter.map {
            data => {
                val obj: JSONObject = JsonUtil.getJsonObject(data)
                val appkey: String = obj.getString("appkey")
                val appregurl: String = obj.getString("appreurl")
                val bdp_uuid: String = obj.getString("bdp_uuid")
                val createtime: String = obj.getString("createtime")
                val dn: String = obj.getString("dn")
                val domain: String = obj.getString("domain")
                val dt: String = obj.getString("dt")
                val isranreg: String = obj.getString("isranreg")
                val regsource: Int = obj.getIntValue("regsource")
                val uid: Int = obj.getIntValue("uid")
                val websiteid: Int = obj.getIntValue("websiteid")
                val regsourceName = regsource match {
                    case 1 => "PC"
                    case 2 => "Mobile"
                    case 3 => "App"
                    case 4 => "WeChat"
                    case _ => "Other"
                }
                (uid, appkey, appregurl, bdp_uuid, createtime, isranreg, regsource, regsourceName, websiteid, dt, dn)
            }
        }
    }

}
