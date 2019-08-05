package com.drodride.training.spark.graphs.properties

case class IndividualProperty (
                              individualID: String,
                              motherID: String,
                              fatherID: String,
                              genre: String,
                              visited: Boolean = false,
                              count: Int = 0,
                              chromosomes: String = ""
                              ) extends VertexProperty
