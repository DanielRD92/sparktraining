package com.drodride.training.spark

import com.drodride.training.spark.graphs.Family
import com.drodride.training.spark.graphs.inputs.Individual
import com.drodride.training.spark.utilities.parallel.CSVReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object GeneticTreeApp {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("GeneticTree")
      .master("local[*]")
      .getOrCreate()

    val dataPath: String = "src/test/resources/genetic_family_tree.csv"
    val data: DataFrame = CSVReader(header = true, delimiter = ",", path = dataPath).csvDF
    data.show()

    val dataRDD: RDD[Individual] = data.rdd.map(r => Individual(r))
    dataRDD.collect().foreach(println(_))

    Family(dataRDD).heritageSimulation.collect().foreach(println(_))






  }

}
