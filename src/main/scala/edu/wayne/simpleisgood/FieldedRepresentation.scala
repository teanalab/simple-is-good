package edu.wayne.simpleisgood

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by fsqcds on 12/10/15.
  */
object FieldedRepresentation {
  def main(args: Array[String]): Unit = {
    val pathToEntityDescriptions = args(0)
    val pathToOutput = args(1)
    val preprocess = args(2) == "pre"
    val triples = args(3) == "tri" // otherwise quads

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
      val obj = Util.cleanPart(Util.extractObject(line, triples), preprocess)

      if (subj.isEmpty || pred.isEmpty || obj.isEmpty) {
        None
      } else {
        if (pred.get.endsWith("name") || pred.get.endsWith("label") || pred.get.endsWith("title"))
          Some(subj.get, (obj, obj))
        else
          Some(subj.get, (None, obj))
      }
    }.groupByKey.flatMap { case (subj, objPairs) => {
      val titleObjs = objPairs.map(pair => pair._1).filter(titleObj => titleObj.nonEmpty)
      val objs = objPairs.map(pair => pair._2)
      Array(
        "<DOC>\n<DOCNO>" + subj + "</DOCNO>\n<TEXT>") ++
        (if (titleObjs.nonEmpty)
          Array("<title>") ++
            titleObjs.map(_.get) ++
            Array("</title>")
        else
          Array[String]()) ++
        Array("<content>") ++
        objs.map(_.get) ++
        Array("</content>\n</TEXT>\n</DOC>")
    }
    }.saveAsTextFile(pathToOutput)
  }
}
