package com.drodride.training.spark.utilities.parallel

import org.apache.spark.sql.{DataFrame, SparkSession}

case class CSVReader (header: Boolean,
                      delimiter: String,
                      path: String) (implicit spark: SparkSession){

  def csvDF: DataFrame = spark.read
    .format("com.databricks.spark.csv")
    .option("header", header)
    .option("delimiter", delimiter)
    .load(path)

}
