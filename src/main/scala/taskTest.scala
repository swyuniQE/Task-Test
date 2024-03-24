import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.functions.{explode, expr, udaf}
import org.apache.spark.sql.{DataFrame,SparkSession}

import java.util

object taskTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SimpleApplication")
      .master("local[*]")
      .getOrCreate();
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    val sizeGiven = if (args.length > 0) args(0).toInt else 3
    printf("size given: %d\n", sizeGiven)

    val Source = Seq(Peer("ABC17969(AB)","1", "ABC17969", 2022),
      Peer("ABC17969(AB)", "2", "CDC52533", 2022),
      Peer("ABC17969(AB)", "3", "DEC59161", 2023),
      Peer("ABC17969(AB)", "4", "F43874", 2022),
      Peer("ABC17969(AB)", "5", "MY06154", 2021),
      Peer("ABC17969(AB)", "6", "MY4387", 2022),
      Peer("AE686(AE)", "7", "AE686", 2023),
      Peer("AE686(AE)", "8", "BH2740", 2021),
      Peer("AE686(AE)", "9", "EG999", 2021),
      Peer("AE686(AE)", "10", "AE0908", 2021),
      Peer("AE686(AE)", "11", "QA402", 2022),
      Peer("AE686(AE)", "12", "OM691", 2022)).toDF()

    println("\nSource data")
    Source.show()
    Source.printSchema()
    /*
    1.    For each peer_id, get the year when peer_id contains id_2,
    for example for ‘ABC17969(AB)’ year is 2022.
     */
    val df1 = answerQ1(spark, Source).cache()
    println("Q1 result")
    df1.show()
    df1.printSchema()

    /*
     2.    Given a size number, for example 3.
     For each peer_id count the number of each year
     (which is smaller or equal than the year in step1).
      */
    val df2 = answerQ2(spark, Source, df1).cache()
    println("Q2 result")
    df2.show()
    df2.printSchema()


    spark.stop()


  }

  def answerQ1(ss: SparkSession, source: DataFrame): DataFrame = {
    import ss.implicits._
    source.filter($"peer_id".contains($"id_2"))
  }

  def answerQ2(ss: SparkSession, source: DataFrame, df1: DataFrame): DataFrame = {
    import ss.implicits._
    val df_tmp = source.groupBy($"peer_id", $"year").count()
    df_tmp.as("a")
      .join(df1.as("b"), $"a.peer_id" === $"b.peer_id", "left_outer")
      .filter($"a.year" <= $"b.year")
      .select($"a.*")
  }

}
