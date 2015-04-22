package org.sagar.spark.examples

import org.apache.spark._

object mapFlapMap{
        def main(args : Array[String]){
                val conf = new SparkConf().setAppName("Map and Flat Map")
                val sc = new SparkContext(conf)
                val lines = sc.parallelize(List("This", "is", "Spark", "demonstration"))
                val flatWords = lines.flatMap(i=>i.split(" "))
                val mapWords = lines.map(i=>i.split(" "))
                println("Flat words" + flatWords + "and count is " + flatWords.count())
                println("Map words" + mapWords + "and count is " + mapWords.count())
                sc.stop()
        }
}
