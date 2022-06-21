package org.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.{SparkConf, SparkContext}

object Delta {
  case class Source(id: String, name: String, sex: String, age: String, money: String)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setAppName("SpeakerDevice")
      .setMaster("local")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    val session = SparkSession.builder().config(conf).getOrCreate()


    val sc = session.sparkContext

    import session.implicits._

//    // sourceDF
//    val sourceDF = sc.textFile("/Users/anker/source.txt")
//      .map(line => {
//        val arr = line.split(",")
//        Source(arr(0), arr(1), arr(2), arr(3), arr(4))
//      }).toDF
//
//    sourceDF.show(10)
//
//    // write DataFrame as Delta
//    sourceDF
//      .write
//      .format("delta")
//      .mode("overwrite")
//      .save("/Users/anker/test")



    val sourceTable = DeltaTable.forPath(session, "/Users/anker/test")

    //sex='男' money字段后拼接'-11'
    sourceTable.update(expr("sex=='男'"), Map("money" -> expr("concat(money, '-11')")))

    sourceTable.toDF.show(false)

  }
}
