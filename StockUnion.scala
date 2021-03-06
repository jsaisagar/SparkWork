package org.sagar.spark.examples

import org.apache.spark._

object StockUnion{
        def main(args: Array[String]){
                val conf = new SparkConf().setAppName("Number of client stocks")
                val context = new SparkContext(conf)
                val data = context.textFile("/Users/saisagarjinka/Downloads/programmingpig-master/data/NYSE_daily")
                val cliRdd = data.map(line =>
                                        line.split("\\t")(1)).filter(line => line.contains("CLI"))
                val cslRdd = data.map(line =>
                                        line.split("\\t")(1)).filter(line => line.contains("CSL"))
                val unionRdd = cliRdd.union(cslRdd)
                unionRdd.persist()
                val counter = unionRdd.count()
                println("THE COUNTER VALUE IS " + counter)
                context.stop
        }
}
