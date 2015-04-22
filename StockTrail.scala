package org.sagar.spark.examples

import org.apache.spark._

import scala.math

object StockTrail{
        def main(args : Array[String]){
                val conf = new SparkConf().setAppName("Stock Trail")
                val context = new SparkContext(conf)
                val data = context.textFile("/Users/saisagarjinka/Downloads/programmingpig-master/data/NYSE_daily")
                val array = data.map(_.split("\\t")(4).toDouble)
                val maxValue = array.fold(0.0)((acc,b) => {if(acc > b) acc else b})
                println("THE MAX VALUE IS ....." + maxValue)
                context.stop()
        }
}
