package com.drodride.training.spark.graphs.inputs

import org.apache.spark.sql.{DataFrame, Row}

case class Individual(
                       individualID: String,
                       motherID: String,
                       fatherID: String,
                       genre: String,
                       chromosomes: String
                     )

object Individual {
  def apply(
             row: Row
           ): Individual =
    new Individual(
      individualID = try row.getAs("individualID").toString catch {
        case e: Exception => ""},
      motherID = try row.getAs("motherID").toString catch {
        case e: Exception => ""},
      fatherID = try row.getAs("fatherID").toString catch {
        case e: Exception => ""},
      genre = try row.getAs("genre").toString catch {
        case e: Exception => ""},
      chromosomes = try row.getAs("chromosomes").toString catch {
        case e: Exception => ""}
    )
}



