package edu.wayne.simpleisgood.playground

import edu.wayne.simpleisgood.Util
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by fsqcds on 12/12/15.
  */
object SubjectCount {
  def main(args: Array[String]): Unit = {
    val pathToEntityDescriptions = args(0)
    val pathToOutput = args(1)

    val conf = new SparkConf().setAppName("BaselineRetrieval")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val descriptions = sc.textFile(pathToEntityDescriptions)

    val subjObjs = descriptions.flatMap { line =>
      Try {
        val splitLine = line.split(" ")
        Util.cleanPart(splitLine(0), false)
      }.toOption
    }.map(word => (word, 1))
      .reduceByKey(_ + _, 1) // 2nd arg configures one task (same as number of partitions)
      .map(item => item.swap) // interchanges position of entries in each tuple
      .sortByKey(false, 1) // 1st arg configures ascending sort, 2nd arg configures one task
      .map(item => item.swap)
      .saveAsTextFile(pathToOutput)
  }
}
