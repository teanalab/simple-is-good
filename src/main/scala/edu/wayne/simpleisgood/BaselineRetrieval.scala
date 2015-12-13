package edu.wayne.simpleisgood

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try


/**
  * Created by fsqcds on 12/10/15.
  */
object BaselineRetrieval {
  def main(args: Array[String]): Unit = {
    val pathToEntityDescriptions = args(0)
    val pathToOutput = args(1)
    val preprocess = args(2) == "pre"

    val conf = new SparkConf().setAppName("BaselineRetrieval")
    val sc = new SparkContext(conf)
    val descriptions = sc.textFile(pathToEntityDescriptions)

    val subjObjs = descriptions.flatMap { line =>
      Try {
        val splitLine = line.split(" ")
        val subj = Util.cleanPart(splitLine(0), false)
        val obj = Util.cleanPart(Util.extractObject(line), preprocess)
        (subj, obj)
      }.toOption
    }.groupByKey.map { case (subj, objs) =>
      "<DOC>\n<DOCNO>" + subj + "</DOCNO>\n<TEXT>\n" + objs.filter(o => o.nonEmpty).mkString("\n") + "\n</TEXT>\n</DOC>"
    }.saveAsTextFile(pathToOutput)
  }
}
