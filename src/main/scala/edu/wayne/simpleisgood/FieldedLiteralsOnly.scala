package edu.wayne.simpleisgood

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by fsqcds on 12/10/15.
  */
object FieldedLiteralsOnly {
  def main(args: Array[String]): Unit = {
    val pathToEntityDescriptions = args(0)
    val pathToOutput = args(1)
    val preprocess = args(2) == "pre" // we add URI to title if preprocess

    val conf = new SparkConf().setAppName("BaselineRetrieval")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val descriptions = sc.textFile(pathToEntityDescriptions)

    val subjObjs = descriptions.flatMap { line =>
      val splitLine = line.split(" ")
      val subj = Util.cleanPart(splitLine(0), false)
      val pred = Util.cleanPart(splitLine(1), false) map {
        _.toLowerCase
      }
      val rawObj = Util.extractObject(line)

      if (subj.isEmpty || pred.isEmpty || rawObj.isEmpty || !rawObj.startsWith("\"")) {
        if (subj.isEmpty)
          None
        else
          Some(subj.get, (None, None))
      } else {
        val obj = Util.cleanPart(rawObj, false)
        if (pred.get.endsWith("name") || pred.get.endsWith("label") || pred.get.endsWith("title"))
          Some(subj.get, (obj, obj))
        else
          Some(subj.get, (None, obj))
      }
    }.groupByKey.flatMap { case (subj, objPairs) => {
      val titleObjs = objPairs.map(pair => pair._1).filter(titleObj => titleObj.nonEmpty)
      val objs = objPairs.map(pair => pair._2).filter(obj => obj.nonEmpty)
      Array("<DOC>\n<DOCNO>" + subj + "</DOCNO>\n<TEXT>") ++
        Array("<title>") ++
        Array(if (preprocess)
          Util.preprocessUri(subj)
        else
          subj) ++
        (if (titleObjs.nonEmpty)
          titleObjs.map(_.get)
        else
          Array[String]()) ++
        Array("</title>") ++
        Array("<content>") ++
        (if (objs.nonEmpty)
          objs.map(_.get)
        else
          Array[String]()) ++
        Array("</content>\n</TEXT>\n</DOC>")
    }
    }.saveAsTextFile(pathToOutput)
  }
}
