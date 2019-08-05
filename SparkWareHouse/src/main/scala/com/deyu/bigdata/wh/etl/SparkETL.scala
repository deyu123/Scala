package com.deyu.bigdata.wh.etl

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.deyu.bigdata.wh.bean.{base_website, member_regtype, vip_level, wxtype, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkETL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkETL")
    val sc = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //    sc.parallelize(testFile.collect(),3)
    //    val textDStream: DStream[String] = ssc.textFileStream("hdfs://hadoop202:9000/user/deyu/odl/Member.log")
    import sparkSession.implicits._
    val textMember: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/Member.log")
    val textPcenterMemViplevel: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/PcenterMemViplevel.log")
    val textWxType: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/WxType.log")
    val textbaseadlog: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/baseadlog.log")
    val textbaswewebsite: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/baswewebsite.log")
    val textmemberRegtype: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/memberRegtype.log")
    val textpcentermempaymoney: RDD[String] = sc.textFile("hdfs://hadoop202:9000/user/deyu/odl/pcentermempaymoney.log")

    val member: RDD[Member_Wite] = textMember.mapPartitions(partition => {
      partition.map { json => {
        val member: Member = JSON.parseObject(json, classOf[Member])
        member.fullname = member.fullname.substring(0, 1) + "XX"
        member.phone = member.phone.substring(0, 3) + "XXXXXXXX"
        member.birthday = member.birthday.replace("-", "")
        member.register = member.register.replace("-", "")
        member.password = "XXXXXX"
//        println(json)
        Member_Wite(member.uid
          , member.ad_id, member.birthday.replace("-", ""),
          member.email, member.fullname.substring(0, 1) + "XX", member.iconurl, member.lastlogin, member.mailaddr, member.memberlevel
          , member.password, member.paymoney, member.phone, member.qq, member.register, member.regupdatetime, member.unitname,
          member.userip, member.zipcode, new SimpleDateFormat("yyyyMMdd").format(new Date()), member.dn)
      }
      }
    })

        val memberDataFrame: DataFrame = member.toDF()
        insertHive(sparkSession, "bdl", "bdl_member", memberDataFrame)

//    println("textPcenterMemViplevel ----------success")
//    val vip_level: RDD[(Int, String, Long, Long, Long, String, String, String, String, String)] = textPcenterMemViplevel.mapPartitions(partition =>
//      partition.map { json =>
//        val vip: vip_level = JSON.parseObject(json, classOf[vip_level])
//
//        (
//          vip.vip_id,
//          vip.vip_level,
//          new SimpleDateFormat("yyyy-MM-dd").parse(vip.start_time).getTime,
//          new SimpleDateFormat("yyyy-MM-dd").parse(vip.end_time).getTime,
//          new SimpleDateFormat("yyyy-MM-dd").parse(vip.start_time).getTime,
//          vip.max_free,
//          vip.min_free,
//          vip.next_level,
//          vip.operator,
//          vip.dn)
//
//      })
//
//          val vip_levelM: DataFrame = vip_level.toDF()
//          insertHive(sparkSession, "bdl", "bdl_vip_level", vip_levelM)
      //

//          println("textWxType ----------success")
//          val wxtype: RDD[(Int, String, String, String, String, String, String, String)] = textWxType.mapPartitions(partition => {
//            partition.map { json => {
//              val wxtype: wxtype = JSON.parseObject(json, classOf[wxtype])
//              wxtype.createtime = wxtype.createtime.replace("-", "")
//              (wxtype.bind_source, wxtype.app_id, wxtype.token_secret, wxtype.wx_name, wxtype.wx_no,
//                wxtype.createtor,
//                wxtype.createtime.replace("-", ""), wxtype.dn)
//            }
//            }
//          })
//          val wxtypeDF: DataFrame = wxtype.toDF()
//          insertHive(sparkSession, "bdl", "bdl_wxtype", wxtypeDF)
      //
//          println("textbaseadlog ----------success ")
//
//          val base_ad: RDD[(Int, String, String)] = textbaseadlog.mapPartitions(partition => {
//            partition.map { json => {
//              val base_ad: base_ad = JSON.parseObject(json, classOf[base_ad])
//              (base_ad.adid, base_ad.adname, base_ad.dn)
//            }
//            }
//          })
//          val wbase_adDF: DataFrame = base_ad.toDF()
//          insertHive(sparkSession, "bdl", "bdl_base_ad", wbase_adDF)

//          println("textbaswewebsite ----------success ")
//    val base_website: RDD[(Int, String, String, Int, Long, String, String)] = textbaswewebsite.mapPartitions(partition => {
//      partition.map { json => {
//        val base_website: base_website = JSON.parseObject(json, classOf[base_website])
//        (base_website.siteid, base_website.sitename, base_website.siteurl, base_website.delete,
//          new SimpleDateFormat("yyyy-MM-dd").parse(base_website.createtime).getTime,
//
//          base_website.creator, base_website.dn)
//      }
//      }
//    })
//          val base_websiteDF: DataFrame = base_website.toDF()
//          insertHive(sparkSession, "bdl", "bdl_base_website", base_websiteDF)

//          println("textmemberRegtype ----------success")
//    val member_regtype: RDD[(Int, String, String, String, Long, String, String, String, String, Int, String, String)] = textmemberRegtype.mapPartitions(partition => {
//      partition.map { json => {
//        val member: member_regtype = JSON.parseObject(json, classOf[member_regtype])
//        val regsourceName: String = member.regsource match {
//          case "1" => "PC"
//          case "2" => "MOBILE"
//          case "3" => "APP"
//          case "4" => "WECHAT"
//          case _ => "Other"
//        }
//        (member.uid, member.appkey, member.appregurl, member.bdp_uuid,
//          new SimpleDateFormat("yyyy-MM-dd").parse(member.createtime).getTime,
//          member.domain, member.isranreg, member.regsource,
//          regsourceName
//          , member.websiteid,
//          new SimpleDateFormat("yyyyMMdd").format(new Date()),
//          member.dn)
//      }
//      }
//    })
//          val member_regtypeDF: DataFrame = member_regtype.toDF()
//          insertHive(sparkSession, "bdl", "bdl_member_regtype", member_regtypeDF)
      //
//          println("textpcentermempaymoney ---------- success")
//          val pcenter: RDD[(Int, String, Int, Int, String, String)] = textpcentermempaymoney.mapPartitions(partition => {
//            partition.map { json => {
//              val pcentermempaymoney: pcentermempaymoney = JSON.parseObject(json, classOf[pcentermempaymoney])
//              (pcentermempaymoney.uid, pcentermempaymoney.paymoney, pcentermempaymoney.siteid,
//                pcentermempaymoney.vip_id, new SimpleDateFormat("yyyyMMdd").format(new Date()), pcentermempaymoney.dn)
//            }
//            }
//          })
//          val pcenterDF: DataFrame = pcenter.toDF()
//          insertHive(sparkSession, "bdl", "bdl_pcentermempaymoney", pcenterDF)


    }

    def insertHive(sparkSession: SparkSession, database: String, tableName: String, dataFrame: DataFrame): Unit = {
      sparkSession.sql("use " + database)
      dataFrame.write.mode("overwrite").insertInto(tableName)
      println("保存：" + tableName + "完成")
    }

  }
