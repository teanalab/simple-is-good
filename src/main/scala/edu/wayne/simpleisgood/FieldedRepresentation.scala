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
    }.groupByKey.map { case (subj, objPairs) => {
      val titleObjs = objPairs.map(pair => pair._1).filter(titleObj => titleObj.nonEmpty).mkString("\n")
      val objs = objPairs.map(pair => pair._2).filter(obj => obj.nonEmpty).mkString("\n")
      "<DOC>\n<DOCNO>" + subj + "</DOCNO>\n<TEXT>\n" +
        (if (titleObjs.nonEmpty)
          "<title>\n" + titleObjs + "\n</title>\n"
        else
          "") +
        (if (objs.nonEmpty)
          "<content>\n" + objs + "\n</content>\n"
        else
          "") + "</TEXT>\n</DOC>"
    }
    }.saveAsTextFile(pathToOutput)
  }
}
