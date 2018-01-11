import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by Chenyu Peng on 10/9/17.
  */
object Chenyu_Peng_SON {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      Console.err.println("./bin/spark-submit ./Chenyu_Peng_SON.jar 1 ./ratings.dat ./users.dat 1200")
      return
    }

    if(args(0).toInt == 1) {
      val conf = new SparkConf().setAppName("Case1").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val ratings = sc.textFile(args(1))
      val users = sc.textFile(args(2))

      val users_info = users.filter(_.contains("M")).map(line1 => {
        val fields1 = line1.split("::")
        (fields1(0).toInt, fields1(1))
      }).keyBy(tup => tup._1)

      val ratings_info = ratings.map(line2 => {
        val fields2 = line2.split("::")
        (fields2(0).toInt, fields2(1).toInt)
      }).keyBy(tup => tup._1)

      val user_movieset = ratings_info.join(users_info).map(f => (f._1, f._2._1._2)).sortBy(c=>c._2,true).groupByKey().map(f =>f._2).collect
      //    user_movieset.repartition(1).saveAsTextFile("result")


      var user_movieset_tran : List[List[Int]] = List()
      var user_movieset_set : List[Int] = List()
      var map_frequentItemsets : List[List[Int]] = List()

      user_movieset_tran = user_movieset.map(f =>f.toList.sorted).toList
      user_movieset_set = user_movieset.flatten.toSet.toList.sorted

      def ComputeSupport(movieset : Set[Int]) : Double = {
        def checkifcontains(itemcontains: List[Int]) : Boolean = {
          movieset.map(f => itemcontains.contains(f)).reduceRight((f1, f2) => f1 && f2)
        }
        val support_num = user_movieset_tran.filter(checkifcontains).size
        support_num.toDouble
      }

      def Apriori(support_threshold : Double) = {
        var Iterations = 2
        var Frequentitemset = user_movieset_set.map(itemset => (Set(itemset), ComputeSupport(Set(itemset)))).filter(CSupport => (CSupport._2 > support_threshold))
        var L_set = Frequentitemset.map(CSupport => CSupport._1)
        while (L_set.size > 0) {
          val C_set_potential = L_set.map(itemset => L_set.map(itemset1 => itemset union itemset1)).reduceRight((itemset1, itemset2) => itemset1 union itemset2).filter(itemset => (itemset.size==Iterations)).distinct
          val C_set_current= C_set_potential.map(itemset => (itemset, ComputeSupport(itemset))).filter(CSupport => (CSupport._2 > support_threshold))
          L_set = C_set_current.map(CSupport => CSupport._1)
          Frequentitemset = Frequentitemset union C_set_current
          Iterations += 1
        }
        //      val a = Frequentitemset.map(f => f._1)
        map_frequentItemsets = Frequentitemset.map(f => f._1.toList.sorted)
      }

      Apriori(args(3).toDouble)

      val rddData = sc.parallelize(map_frequentItemsets, 1)
      val frequentmoviesets = rddData.map(f => (f.size, f)).groupByKey().sortBy(c=>c._1,true).map(f =>(f._2).toList)
      val frequentmoviesets_String = frequentmoviesets.map(x1=>x1.map(x2=>x2.mkString("(",",",")")).mkString(","))

      frequentmoviesets_String.saveAsTextFile("Chenyu_Peng_SON.case" + args(0) + "_" + args(3) + ".txt")
      }

    else if(args(0).toInt == 2){
      val conf = new SparkConf().setAppName("Case2").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val ratings = sc.textFile(args(1))
      val users = sc.textFile(args(2))

      val users_info = users.filter(_.contains("F")).map(line1 => {
        val fields1 = line1.split("::")
        (fields1(0).toInt, fields1(1))
      }).keyBy(tup => tup._1)

      val ratings_info = ratings.map(line2 => {
        val fields2 = line2.split("::")
        (fields2(0).toInt, fields2(1).toInt)
      }).keyBy(tup => tup._1)

      val movie_userset = ratings_info.join(users_info).map(f => (f._2._1._2, f._1)).sortBy(c=>c._2,true).groupByKey().map(f =>f._2).collect
      //    movie_userset.repartition(1).saveAsTextFile("result")


      var movie_userset_tran : List[List[Int]] = List()
      var movie_userset_set : List[Int] = List()
      var map_frequentItemsets : List[List[Int]] = List()

      movie_userset_tran = movie_userset.map(f =>f.toList.sorted).toList
      movie_userset_set = movie_userset.flatten.toSet.toList.sorted

      def ComputeSupport(userset : Set[Int]) : Double = {
        def checkifcontains(itemcontains: List[Int]) : Boolean = {
          userset.map(f => itemcontains.contains(f)).reduceRight((f1, f2) => f1 && f2)
        }
        val support_num = movie_userset_tran.filter(checkifcontains).size
        support_num.toDouble
      }

      def Apriori(support_threshold : Double) = {
        var Iterations = 2
        var Frequentitemset = movie_userset_set.map(itemset => (Set(itemset), ComputeSupport(Set(itemset)))).filter(CSupport => (CSupport._2 > support_threshold))
        var L_set = Frequentitemset.map(CSupport => CSupport._1)
        while (L_set.size > 0) {
          val C_set_potential = L_set.map(itemset => L_set.map(itemset1 => itemset union itemset1)).reduceRight((itemset1, itemset2) => itemset1 union itemset2).filter( itemset => (itemset.size==Iterations)).distinct
          val C_set_current= C_set_potential.map(itemset => (itemset, ComputeSupport(itemset))).filter(CSupport => (CSupport._2 > support_threshold))
          L_set = C_set_current.map(CSupport => CSupport._1)
          Frequentitemset = Frequentitemset union C_set_current
          Iterations += 1
        }
        //      val a = Frequentitemset.map(f => f._1)
        map_frequentItemsets = Frequentitemset.map(f => f._1.toList.sorted)
      }

      Apriori(args(3).toDouble)

      val rddData = sc.parallelize(map_frequentItemsets, 1)
      val frequentusersets = rddData.map(f => (f.size, f)).groupByKey().sortBy(c=>c._1,true).map(f =>f._2.toList)
      val frequentmoviesets_String = frequentusersets.map(x1=>x1.map(x2=>x2.mkString("(",",",")")).mkString(","))

      frequentmoviesets_String.saveAsTextFile("Chenyu_Peng_SON.case" + args(0) + "_" + args(3) + ".txt")
    }

    else{
      Console.err.println("Case number error, 1 or 2")
      return
    }

  }
}