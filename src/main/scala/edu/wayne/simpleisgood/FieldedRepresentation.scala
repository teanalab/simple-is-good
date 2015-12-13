package edu.wayne.simpleisgood

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try


/**
  * Created by fsqcds on 12/10/15.
  */
object FieldedRepresentation {
  def main(args: Array[String]): Unit = {
    val pathToEntityDescriptions = args(0)
    val pathToOutput = args(1)
    val preprocess = args(2) == "pre"

    val conf = new SparkConf().setAppName("BaselineRetrieval")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val descriptions = sc.textFile(pathToEntityDescriptions)

    val subjObjs = descriptions.flatMap { line =>
      val splitLine = line.split(" ")
      Try {
        val subj = Util.cleanPart(splitLine(0), false)
        val pred = Util.cleanPart(splitLine(1), false).toLowerCase
        val obj = Util.cleanPart(Util.extractObject(line), preprocess)

        if (pred.endsWith("name") || pred.endsWith("label") || pred.endsWith("title"))
          (subj, (obj, obj))
        else
          (subj, ("", obj))
      }.toOption
    }.groupByKey.flatMap { case (subj, objPairs) => {
      val titleObjs = objPairs.map(pair => pair._1).filter(titleObj => titleObj.nonEmpty)
      val objs = objPairs.map(pair => pair._2).filter(obj => obj.nonEmpty)
      Array(
        "<DOC>\n<DOCNO>" + subj + "</DOCNO>\n<TEXT>") ++
        (if (titleObjs.nonEmpty)
          Array("<title>") ++
            titleObjs ++
            Array("</title>")
        else
          Array[String]()) ++
        (if (objs.nonEmpty)
          Array("<content>") ++
            objs ++
            Array("</content>")
        else
          Array[String]()) ++
        Array("</TEXT>\n</DOC>")
    }
    }.saveAsTextFile(pathToOutput)
  }
}
