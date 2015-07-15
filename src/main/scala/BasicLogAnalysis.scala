package com.klyk.webserverloganalysis

import com.klyk.clfparser._
import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions.Descending

/**
 * Created by klyk on 7/14/15.
 */
object BasicLogAnalysis {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputdirectory] [outputdirectory]")
      System.exit(1)
    }
    val master = args(0)
    val inputFile = args(1)
    val outputFile = args(2)

    val conf = new SparkConf().setMaster(master).setAppName("basicLogAnalysis")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("/tmp/spark/")

    println("-------------Attach debugger now!--------------")
    Thread.sleep(10000)

    //Part 1 - read logs from July and parse

    val parser = new ApacheCLFParser
    val emptyApacheRecord = new ApacheCLFRecord("","","","","","","")

    val julylogs = sc.textFile(inputFile)

    val julylogsObj = julylogs
      .map(line => parser.parseRecord(line)).cache()

    val numOfBadRecords = julylogsObj
      .filter(element => element == None).count()

    //compute content size returned over time in bytes
    val content_size = julylogsObj
      .map(element => element.getOrElse(emptyApacheRecord).bytesSent.toLong)
      .reduce(,_+_)

    //Part 2 basic analysis

    //how many 404 codes in july
    val responseCodes = julylogsObj
      .map(element => (element.getOrElse(emptyApacheRecord).httpStatusCode,1))
      .filter(pair => pair._1 != "404")
      .reduceByKey(_+_)

    //frequent hosts - top 20 visitors
    val top20visitors = julylogsObj
      .map(element => (element.getOrElse(emptyApacheRecord).clientIpAddress,1))
      .reduceByKey(_+_)
      .map(element => element.swap)
      .takeOrdered(20)(Ordering[Int].reverse.on(pair=>pair._1))

    //number of unique hosts
    val hosts = julylogsObj
      .map(element => (element.getOrElse(emptyApacheRecord).clientIpAddress,1))
      .groupByKey()
      .count()

    //Part 3 advanced analysis

    //daily unique hosts
    val dayToHostPairTuple = julylogsObj
      .map(element=> (element.getOrElse(emptyApacheRecord).dateTime,element.getOrElse(emptyApacheRecord).clientIpAddress))

    val dayGroupedHosts = dayToHostPairTuple
      .groupByKey()




  }
}
