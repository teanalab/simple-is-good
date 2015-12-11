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
        val obj = Util.cleanPart(splitLine(2), preprocess)

        if (pred.endsWith("name") || pred.endsWith("label") || pred.endsWith("title"))
          (subj, (Some(obj), obj))
        else
          (subj, (None, obj))
      }.toOption
    }.reduceByKey { (a, b) =>
      (if (a._1.isEmpty && b._1.isEmpty)
        None
      else
        Some(Seq(a._1, b._1).flatten.mkString("\n")),
        a._2 + "\n" + b._2)
    }.map { case (subj, (titleObjs, contentObjs)) =>
      "<DOC>\n<DOCNO>\n" + subj + "\n<TEXT>\n" +
        (titleObjs match {
          case Some(title) => "<title>\n" + title + "\n</title>\n"
          case None => ""
        }) +
        "<content>\n" + contentObjs + "\n</content>\n<TEXT>"
    }.saveAsTextFile(pathToOutput)
  }
}
