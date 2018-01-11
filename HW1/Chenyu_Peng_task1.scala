import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by Chenyu Peng on 9/18/17.
  */
object Chenyu_Peng_task1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ratings = sc.textFile("./ratings.dat")
    val users = sc.textFile("./users.dat")

    val users_info = users.map(line1=>{
      val fields1 = line1.split("::")
      (fields1(0).toInt, fields1(1))
    }).keyBy(tup => tup._1)

    val ratings_info = ratings.map(line2=>{
      val fields2 = line2.split("::")
      (fields2(0).toInt, fields2(1).toInt, fields2(2).toDouble)
    }).keyBy(tup => tup._1)

    val result1 = ratings_info.join(users_info).map(f => ((f._2._1._2, f._2._2._2), f._2._1._3)).sortBy(c=>c._1,true)

    val ratings_avg = result1.groupByKey().map(data=> {
      val avg = data._2.sum / data._2.size
      (data._1, avg)
    }).sortBy(c=>c._1,true).map(f => f._1._1 + "," + f._1._2 + "," + f._2)

    ratings_avg.repartition(1).saveAsTextFile("Chenyu_Peng_result_task1")
  }

}