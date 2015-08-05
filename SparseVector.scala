package com.sagar.learning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg._

object sparse {
  def sparsing(listTerms: List[String], mapAllTerms: scala.collection.Map[String, Long]) = {
    var sequence = Seq[(Int,Double)]()
    listTerms.map {
      x =>
        val z = mapAllTerms.get(x)
        z match {
          case Some(temp) => sequence = sequence :+ (temp.toInt,1.0)
          case _ => println("Not Found")
        }
    }
    sequence = sequence.distinct
    val sparsedVec = Vectors.sparse(1000000, sequence)
    sparsedVec
  }
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    
    sparkConf.setAppName("SparseVector")
    
    val sc = new SparkContext(sparkConf)
    
    val text = sc.textFile("/Users/saisagarjinka/Downloads/u.item").map(_.split("\\|")).map(x => x(1)).map(_.replaceAll("\\(\\d+\\)","").trim)
    
    val listTerms = text.map(_.split(" ").toList)
     
    val allTerms = listTerms.flatMap(x => x)

    val allTermsWithid = allTerms.zipWithUniqueId

    val mapAllTerms = allTermsWithid.collectAsMap
    
    val result = listTerms.map(x => sparsing(x, mapAllTerms))
 	
    result.foreach(println)
  }
}
