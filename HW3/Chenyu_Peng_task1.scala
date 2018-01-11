import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import scala.math._

object Chenyu_Peng_task1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val accumulator01= sc accumulator(0,"broadcast01")
    val accumulator12= sc accumulator(0,"broadcast12")
    val accumulator23= sc accumulator(0,"broadcast23")
    val accumulator34= sc accumulator(0,"broadcast34")
    val accumulator4x= sc accumulator(0,"broadcast4x")
    //    val accumulator= sc accumulator(0,"broadcast")

    val start = System.nanoTime()

    val total_data = sc.textFile(args(0))
    val test_data = sc.textFile(args(1))

    val total_data_RDD = sc.makeRDD(total_data.map(lines => {
      val fields = lines.split(',')
      (fields(0),fields(1),fields(2))
    }).toLocalIterator.drop(1).toSeq)
    val total_data_Set = total_data_RDD.map(f => (f._1, f._2)).toLocalIterator.toSet

    val test_data_RDD = sc.makeRDD(test_data.map(lines => {
      val fields = lines.split(',')
      (fields(0),fields(1))
    }).toLocalIterator.drop(1).toSeq)
    val test_data_Set = test_data_RDD.toLocalIterator.toSet

    val model_data_Set = total_data_Set -- test_data_Set
    val model_data_RDD = sc.makeRDD(model_data_Set.toSeq)

    val total_data_Map = total_data_RDD.map(f => ((f._1, f._2),f._3)).collect().toMap

    val model_data = model_data_RDD.map(f => (f._1, f._2, total_data_Map.get(f))).map{case (user, movie, Some(rate)) =>(user, movie, rate)}

    val test_data_rates = test_data_RDD.map(f => (f._1, f._2, total_data_Map.get(f))).map{case (user, movie, Some(rate)) =>(user, movie, rate)}

    val user_rate_average = model_data.map(f => (f._1.toInt, f._3.toDouble)).groupByKey().map(f => (f._1, f._2.sum/f._2.size))//.sortByKey()

    val model_data_ratings = model_data.map(f =>f match { case (user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val rank = 5
    val numIterations = 5
    val model = ALS.train(model_data_ratings, rank, numIterations, 0.01)

    val test_data_predictions = model.predict(test_data_RDD.map(f => (f._1.toInt, f._2.toInt))).map { case Rating(user, product, rate) => (user, product, rate)}

    val test_data_predictions_set = test_data_predictions.map(f => (f._1, f._2)).toLocalIterator.toSet
    val test_data_predictions_lost = test_data_Set.map(f =>(f._1.toInt, f._2.toInt)) -- test_data_predictions_set
    val test_data_predictions_lost_rate = sc.makeRDD(test_data_predictions_lost.toSeq).join(user_rate_average).map(f =>(f._1, f._2._1, f._2._2)).toLocalIterator.toSet
    val test_data_predictions_total_set = test_data_predictions.toLocalIterator.toSet ++ test_data_predictions_lost_rate
    val test_data_predictions_total = sc.makeRDD(test_data_predictions_total_set.toSeq).map(f => ((f._1, f._2),f._3))//.sortByKey()

    val end = System.nanoTime()

    val test_data_ratesAndPreds = test_data_rates.map { case (user, product, rate) => ((user.toInt, product.toInt), rate.toDouble)}.join(test_data_predictions_total)

    val MSE = test_data_ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    val RMSE = sqrt(MSE)

    val error_num = test_data_ratesAndPreds.foreach { case ((user, product), (r1, r2)) =>
      val err = abs(r1 - r2)
      if(err < 1 && err >= 0){
        accumulator01.add(1)
      }
      else if(err < 2 && err >= 1){
        accumulator12.add(1)
      }
      else if(err < 3 && err >= 2){
        accumulator23.add(1)
      }
      else if(err < 4 && err >= 3){
        accumulator34.add(1)
      }
      else if(err >= 4){
        accumulator4x.add(1)
      }
      //      accumulator.add(1)
    }


    println(">=0 and <1: " + accumulator01.value)
    println(">=1 and <2: " + accumulator12.value)
    println(">=2 and <3: " + accumulator23.value)
    println(">=3 and <4: " + accumulator34.value)
    println(">=4: " + accumulator4x.value)
    println("Mean Squared Error = " + RMSE)
    //    println(accumulator.value)

    println("The total execution time taken is " + (end - start)* pow(10, -9) + " sec.")

    val head = List("UserId,MovieId,Pred_rating")

    val outputfile = head ++ (test_data_predictions_total.sortByKey().map(f => f._1._1 +","+ f._1._2+","+ f._2).toLocalIterator.toList)
    val pred_outputfile = sc.makeRDD(outputfile)

    pred_outputfile.repartition(1).saveAsTextFile("Chenyu_Peng_result_task1.txt")
  }
}
