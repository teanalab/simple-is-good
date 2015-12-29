package edu.wayne.simpleisgood

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by fsqcds on 12/10/15.
  */
object URIasTitle {
  def main(args: Array[String]): Unit = {
    val pathToEntityDescriptions = args(0)
    val pathToOutput = args(1)

    val conf = new SparkConf().setAppName("BaselineRetrieval")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val descriptions = sc.textFile(pathToEntityDescriptions)

    val subjObjs = descriptions.flatMap { line =>
      val splitLine = line.split(" ")
      val subj = Util.cleanPart(splitLine(0), false)
      val obj = Util.cleanPart(Util.extractObject(line), false)
      for (a <- subj; b <- obj) yield (a, b)
    }.groupByKey.flatMap { case (subj, objs) =>
      Array("<DOC>\n<DOCNO>" + subj + "</DOCNO>\n<TEXT>") ++
        Array("<title>") ++
        subj ++
        Array("</title>\n<content>") ++
        objs.filter(o => o.nonEmpty) ++
        Array("</content>\n</TEXT>\n</DOC>")
    }.saveAsTextFile(pathToOutput)
  }
}
