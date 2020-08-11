package com.wang930126.education.etl

import com.alibaba.fastjson.JSONObject
import com.wang930126.education.util.JsonUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 里面的方法对ods层各种表进行ETL处理
  */
object EducationETLService {

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
