package com.drodride.training.spark.utilities

import org.apache.hive.common.util.Murmur3

object Utils {

  def generateID(str: String): Long = {
    val strByte = str.toCharArray.map(_.toByte)
    val len = strByte.length
    Murmur3.hash64(strByte, len, Int.MaxValue)
  }

}
