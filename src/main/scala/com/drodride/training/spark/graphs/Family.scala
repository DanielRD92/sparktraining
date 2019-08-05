package com.drodride.training.spark.graphs

import com.drodride.training.spark.graphs.inputs.Individual
import com.drodride.training.spark.graphs.properties.{GraphMessage, IndividualProperty, VertexProperty}
import com.drodride.training.spark.utilities.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import scala.util.Random

case class Family (relatives: RDD[Individual])(implicit spark: SparkSession) {
  private def getGraph: Graph[VertexProperty, String] = {

    val edges: RDD[Edge[String]] =
      // Filters those relatives which doesn't have known parents (heads)
      relatives.filter(ind => ind.motherID!="" || ind.fatherID!="" ).flatMap(ind => {
        List(
            Edge(
              generateID(ind.motherID), //Origin
              generateID(ind.individualID), //Destiny
              "familySon" //Atribute
            ),
            Edge(
              generateID(ind.fatherID),
              generateID(ind.individualID),
              "familySon"
            )
          )
      })

    val vertices: RDD[(Long, VertexProperty)] =
      relatives.map(ind => {
        (
          generateID(ind.individualID),
          IndividualProperty(
            individualID = ind.individualID,
            motherID = ind.motherID,
            fatherID = ind.fatherID,
            genre = ind.genre,
            chromosomes = ind.chromosomes)
            .asInstanceOf[VertexProperty]
        )
      })

    Graph(vertices,edges)
  }

  private def genSelection(message: GraphMessage, individual: IndividualProperty): String = {
    val pair: (String,String) = message.chromosomes

    val seed: Long = System.currentTimeMillis()
    val rndGen1: Random = new Random(seed-63036016.toLong)
    val rndGen2: Random = new Random(seed)

    val a= {
      val parent1: Array[String] = pair._1.split("")
      val rnd: Int = rndGen1.nextInt(2)

      if (individual.genre=="F" && parent1.map(_.toLowerCase).contains("y"))
        parent1.filter(_.toLowerCase != "y").head
      else if (individual.genre=="M" && parent1.map(_.toLowerCase).contains("y"))
        parent1.filter(_.toLowerCase == "y").head
      else parent1(rnd)

    }
    val b= {
      val parent2: Array[String] = pair._2.split("")
      val rnd: Int = rndGen1.nextInt(2)

      if (individual.genre=="F" && parent2.map(_.toLowerCase).contains("y"))
        parent2.filter(_.toLowerCase != "y").head
      else if (individual.genre=="M" && parent2.map(_.toLowerCase).contains("y"))
        parent2.filter(_.toLowerCase == "y").head
      else parent2(rnd)

    }
    a+b
  }

  private val initialMessage = GraphMessage("", ("",""))

  private def vprog(
                     vertexId: VertexId,
                     vertexValue: VertexProperty,
                     message: GraphMessage
                   ): VertexProperty = {
    vertexValue match {
        //In case the VertexProperty reached was extended by IndividualProperty
      case v: IndividualProperty =>
        // In case the individual is a son/daughter and receives message from a parent
        if (message.origin != "" && (v.motherID !="" || v.fatherID != ""))
            IndividualProperty(
              individualID = v.individualID,
              visited = true,
              motherID = v.motherID,
              fatherID= v.fatherID,
              genre=v.genre,
              count = v.count + 1,
              chromosomes = genSelection(message, v)
          )
        // In case the individual is family head and receives initial message
        else if (message.origin == "" && (v.motherID =="" || v.fatherID == ""))
          IndividualProperty(
            individualID = v.individualID,
            visited = true,
            motherID = v.motherID,
            fatherID= v.fatherID,
            genre=v.genre,
            count = v.count + 1,
            chromosomes = v.chromosomes
          )
          // Rest of cases (not really possible but it gives solidity)
        else
          IndividualProperty(
            individualID = v.individualID,
            motherID = v.motherID,
            fatherID= v.fatherID,
            genre=v.genre
          )
        //In case the VertexProperty reached was extended by other (not actually possible)
      case _ => new VertexProperty
    }
  }



  private def sendMessage(
                         triplet: EdgeTriplet[VertexProperty, String]
                         ): Iterator[(VertexId, GraphMessage)] ={
    val source = triplet.srcAttr

    source match
      {
      case src: IndividualProperty =>
        if (src.visited)
          Iterator((
            triplet.dstId,
            GraphMessage(src.individualID, (src.chromosomes,""))
          ))
        else Iterator.empty
      case _ => Iterator.empty
    }
  }

  private def mergeMessage(
                          a: GraphMessage,
                          b: GraphMessage
                          ): GraphMessage =
    GraphMessage(
      origin = a.origin+"|"+b.origin,
      chromosomes = (a.chromosomes._1,b.chromosomes._1)
    )

  def heritageSimulation = getGraph.pregel(initialMessage, 10, EdgeDirection.Out)(vprog, sendMessage, mergeMessage).vertices
}

