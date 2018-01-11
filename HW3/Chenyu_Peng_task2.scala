import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._

object Chenyu_Peng_task2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val accumulator01= sc accumulator(0,"broadcast01")
    val accumulator12= sc accumulator(0,"broadcast12")
    val accumulator23= sc accumulator(0,"broadcast23")
    val accumulator34= sc accumulator(0,"broadcast34")
    val accumulator4x= sc accumulator(0,"broadcast4x")

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
    val model_data_rates = model_data_RDD.map(f => (f._1, f._2, total_data_Map.get(f))).map{
      case (user, movie, Some(rate)) =>(user, movie, rate)}
    val test_data_rates = test_data_RDD.map(f => (f._1, f._2, total_data_Map.get(f))).map{
      case (user, movie, Some(rate)) =>(user, movie, rate)}

    val test_data_ratings = test_data_rates.map(f =>f match { case (user, item, rate) =>
      (user.toInt, item.toInt, rate.toDouble)
    })

    val user_rate_average = model_data_rates.map(f => (f._1.toInt, f._3.toDouble)).groupByKey().map(f => (f._1, f._2.sum/f._2.size))
    val user_rate_average_Map = user_rate_average.map(f => (f._1, f._2)).collect().toMap

    val model_data_rates_norm = model_data_rates.map(f => (f._1.toInt, f._2.toInt, f._3.toDouble)).map(f =>
      (f._1, f._2, f._3, user_rate_average_Map.get(f._1))).map{
      case (user, movie, rate, Some(average_rate)) =>(user, movie, rate-average_rate)}

    val model_data_rates_norm_tran = model_data_rates_norm.map(f => (f._2, f._1, f._3)).keyBy(tup => tup._1)
    val model_data_ratingPairs = model_data_rates_norm_tran.join(model_data_rates_norm_tran)


    val user_tempVectorCalcs = model_data_ratingPairs.map(data => {
      val key = (data._2._1._2, data._2._2._2)
      val value = (data._2._1._3 * data._2._2._3, data._2._1._3, data._2._2._3, pow(data._2._1._3,2), pow(data._2._2._3,2))
      (key, value)
    })

    val user_vectorCalcs = user_tempVectorCalcs.groupByKey().map(data => {
      val key = data._1
      val vals = data._2
      val dotProduct = vals.map(f => f._1).sum
      val rating1_Square_sum = vals.map(f => f._4).sum
      val rating2_Square_sum = vals.map(f => f._5).sum
      (key, (dotProduct, rating1_Square_sum, rating2_Square_sum))
    }).filter(x=> (x._2._2 != 0) && (x._2._3 != 0))

    val userset_similarity_weights = user_vectorCalcs.map(data => ((data._1._1, data._1._2), data._2._1/(sqrt(data._2._2)*sqrt(data._2._3))))

    val model_data_rates_tran = model_data_rates.map(f => (f._2.toInt, (f._1.toInt, f._3.toDouble)))

    val test_data_RDD_tran = test_data_RDD.map(f => (f._2.toInt, f._1.toInt))

    val userpair_rate_norm = test_data_RDD_tran.join(model_data_rates_tran).map(f => (f._2._1, f._2._2._1, f._1, f._2._2._2, user_rate_average_Map.get(f._2._2._1))).map{
      case (user1, user2, movie, rate, Some(average_rate)) =>((user1,user2), (movie, rate, rate-average_rate))}

    val userpair_weight_rate = userpair_rate_norm.join(userset_similarity_weights).map(f => ((f._1._1, f._2._1._1),(f._2._2, f._2._2*f._2._1._3)))

    val user_weight_rate = userpair_weight_rate.groupByKey().map(data => {
      val key = data._1
      val vals = data._2
      val weight_sum = vals.map(f => abs(f._1)).sum
      val weight_rate_sum = vals.map(f => f._2).sum
      (key, (weight_sum, weight_rate_sum))
    })

    val predictResult = user_weight_rate.map(f => (f._1._1, f._1._2, user_rate_average_Map.get(f._1._1), f._2._2/f._2._1)).map{
      case (user, movie, Some(average_rate), weight_rate) =>((user, movie, average_rate+weight_rate))}//.sortBy(f=>f._1)

    val test_data_predictions_set = predictResult.map(f => (f._1, f._2)).toLocalIterator.toSet
    val test_data_predictions_lost = test_data_Set.map(f =>(f._1.toInt, f._2.toInt)) -- test_data_predictions_set
    val test_data_predictions_lost_rate = sc.makeRDD(test_data_predictions_lost.toSeq).map(f => (f._1, f._2)).join(user_rate_average).map(f =>(f._1, f._2._1, f._2._2)).toLocalIterator.toSet
    val test_data_predictions_total_set = predictResult.toLocalIterator.toSet ++ test_data_predictions_lost_rate
    val test_data_predictions_total = sc.makeRDD(test_data_predictions_total_set.toSeq).map(f => ((f._1, f._2),f._3))

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
    }

    println(">=0 and <1: " + accumulator01.value)
    println(">=1 and <2: " + accumulator12.value)
    println(">=2 and <3: " + accumulator23.value)
    println(">=3 and <4: " + accumulator34.value)
    println(">=4: " + accumulator4x.value)
    println("Mean Squared Error = " + RMSE)

    println("The total execution time taken is " + (end - start)* pow(10, -9) + " sec.")

    val head = List("UserId,MovieId,Pred_rating")

    val outputfile = head ++ (test_data_predictions_total.sortByKey().map(f => f._1._1 +","+ f._1._2+","+ f._2).toLocalIterator.toList)
    val pred_outputfile = sc.makeRDD(outputfile)

    pred_outputfile.repartition(1).saveAsTextFile("Chenyu_Peng_result_task2.txt")
  }
}
