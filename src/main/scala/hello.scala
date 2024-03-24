import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{DataFrame,SparkSession}

import java.util

case class Peer(peer_id: String, id_1: String, id_2: String, year: Integer)
object hello {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Hello")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)
    // 创建RDD（通过集合）
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 对数据进行转换
    val rdd2: RDD[Int] = rdd1.map(_ + 1)
    // 打印结果
    rdd2.collect().foreach(println)

    //关闭 Spark
    sc.stop()
  }

}
