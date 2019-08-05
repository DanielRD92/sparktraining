package com.drodride.training.spark

import com.drodride.training.spark.utilities.Utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Training")
      .master("local[*]")
      .getOrCreate()
  }

}
