package org.demo

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.{SparkConf, SparkContext}

object Delta {

  case class Source(id: String, name: String, sex: String, age: String, money: String)

  case class Source1(id: String, name: String, money: String)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setAppName("SpeakerDevice")
      .setMaster("local")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    val session = SparkSession.builder().config(conf).getOrCreate()


    val sc = session.sparkContext

    import session.implicits._

    /**
     * 查询文件写入
     */
    // sourceDF
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


    /**
     * 给sex='男' money字段后拼接'-11'
     */
    //   val sourceTable = DeltaTable.forPath(session, "/Users/anker/test")
    //sex='男' money字段后拼接'-11'
    //    sourceTable.update(expr("sex=='男'"), Map("money" -> expr("concat(money, '-11')")))
    //
    //    sourceTable.toDF.show(false)

    /**
     * 删除sex="男"的数据
     */

    //   val sourceTable = DeltaTable.forPath(session, "/Users/anker/test")
    //    sourceTable.delete(expr("sex=='男'"))
    //
    //    sourceTable.toDF.show(false)


    /**
     * merge表写入snappy.parquet格式
     */
    //        val sourceDF = sc.textFile("/Users/anker/merge.txt")
    //          .map(line => {
    //            val arr = line.split(",")
    //            Source1(arr(0), arr(1), arr(2))
    //          }).toDF
    //
    //        sourceDF.show(10)
    //
    //        // write DataFrame as Delta
    //        sourceDF
    //          .write
    //          .format("delta")
    //          .mode("overwrite")
    //          .save("/Users/anker/test1")


    /**
     * 二个表合并merge
     */
    //    val sourceTable = DeltaTable.forPath(session, "/Users/anker/test")
    ////    val mergeTable = DeltaTable.forPath(session, "/Users/anker/test1")
    ////
    ////    sourceTable.as("source")
    ////      .merge(mergeTable.toDF.coalesce(1).as("merge"), "source.id=merge.id")
    ////      .whenMatched()
    ////      .update(Map("money" -> col("merge.money")))
    ////      .execute()
    ////
    ////    sourceTable.toDF.show(false)

    /**
     * 打印
     */
    //    DeltaTable.forPath(session, "/Users/anker/test").toDF.show(false)
    //    DeltaTable.forPath(session, "/Users/anker/test1").toDF.show(false)

    /**
     * 版本追溯
     */
    session.read.format("delta").option("versionAsOf", 1).load("/Users/anker/test").show(false)

  }
}
