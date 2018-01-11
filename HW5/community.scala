import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map, Queue, Set}
import scala.math._

object community {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("community").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))

    val data_RDD = sc.makeRDD(data.map(lines => {
      val fields = lines.split(',')
      (fields(0),fields(1))
    }).toLocalIterator.drop(1).toSeq).map(f => (f._2.toInt, f._1.toInt))

    val edges = data_RDD.join(data_RDD).map(f => ((f._2._1, f._2._2),1)).reduceByKey(_+_).filter(_._2 >= 3).map(f => f._1)
    val graph = edges.groupByKey().map(f => (f._1, f._2.toList)).collect()
    val graph_mutable = collection.mutable.Map(graph.toSeq: _*)

    def BFS(vertex : Int, graph : Map[Int,List[Int]]) : (Map[Int, ListBuffer[Int]], Map[Int, Double]) = {
      val node_depth: Map[Int, Int] = Map(vertex -> 0)
      val path_num: Map[Int, Double] = Map(vertex -> 1)
      val BFS_queue = new Queue[Int]
      val BFS_tree : Map[Int, ListBuffer[Int]] = Map(vertex -> ListBuffer())

      BFS_queue.enqueue(vertex)

      while(BFS_queue.nonEmpty) {
        val vertexId = BFS_queue.dequeue()
        val listBuf = ListBuffer.empty[Int]
        val leaf_node = graph.apply(vertexId)
        for(node <- leaf_node) {
          if(!node_depth.contains(node) || node_depth(node) > node_depth(vertexId)) {
            listBuf.append(node)
            if(!node_depth.contains(node)) {
              BFS_queue.enqueue(node)
            }
            if(!path_num.contains(node)) {
              path_num.update(node, 1)
            }
            else {
              path_num.update(node, path_num.apply(node)+1)
            }
            node_depth.update(node, node_depth.apply(vertexId)+1)
          }
        }
        BFS_tree.update(vertexId, listBuf)
      }
      (BFS_tree, path_num)
    }

    def Tree_reverse(tree : Map[Int, ListBuffer[Int]]) : Map[Int, List[Int]] = {
      val tree_reversed = tree.map(f => (f._2.map(x => (x, f._1)).toList)).toList.flatten
      val tmp = sc.makeRDD(tree_reversed).groupByKey().map(f => (f._1, f._2.toList)).collect()
      val tree_reversed_mutable = collection.mutable.Map(tmp.toSeq: _*)
      tree_reversed_mutable
    }

    def Compute_credit(path_num : Map[Int, Double], tree : Map[Int, ListBuffer[Int]], tree_reversed : Map[Int, List[Int]]) : Map[(Int, Int), Double] = {
      val BFS_queue = new Queue[Int]
      val vertex : Map[Int, ListBuffer[Double]] = Map()
      val edge : Map[(Int, Int), Double] = Map()
      tree.foreach(f =>
        if(f._2.isEmpty) {
          BFS_queue.enqueue(f._1)
          vertex.update(f._1, ListBuffer(0.0))
        })
      while(BFS_queue.nonEmpty) {
        val vertexId = BFS_queue.dequeue()
        if(tree_reversed.contains(vertexId)) {
          val father_node = tree_reversed.apply(vertexId)
          for (node <- father_node) {
            if(!vertex.contains(node)) {
              edge.update((vertexId, node), (path_num.apply(node) / path_num.apply(vertexId)) * (vertex.apply(vertexId).sum + 1.0))
              vertex.update(node, ListBuffer((path_num.apply(node) / path_num.apply(vertexId)) * (vertex.apply(vertexId).sum + 1.0)))
            }
            else {
              edge.update((vertexId, node), (path_num.apply(node) / path_num.apply(vertexId)) * (vertex.apply(vertexId).sum + 1.0))
              vertex.update(node, vertex.apply(node).+=((path_num.apply(node) / path_num.apply(vertexId)) * (vertex.apply(vertexId).sum + 1.0)))
            }
            if (!BFS_queue.contains(node)) {
              BFS_queue.enqueue(node)
            }
          }
        }
      }
      edge
    }

    def Compute_betweenness(graph : Map[Int, List[Int]]) : Map[(Int, Int), Double] = {
      val node_list = graph.map(_._1).toList
      var i = 0
      val betweenness : Map[(Int, Int), Double] = Map()
      for (node <- node_list) {
        i = i+1
//        println("this is " +i + " node")
        val (tree, path_num) = BFS(node, graph)
        val tree_reverse = Tree_reverse(tree)
        val credit = Compute_credit(path_num, tree, tree_reverse)
//        credit.foreach(println)
//        betweenness ++= credit
        credit.foreach(f => {
          if(!betweenness.contains(f._1)) {
            betweenness.update(f._1, f._2)
          }
          else {
            betweenness.update(f._1, betweenness.apply(f._1)+f._2)
          }
        })
      }
      betweenness
    }

    def Connected_component(vertex : Int, graph : Map[Int,List[Int]]) : Set[Int] = {
      val queue = new Queue[Int]
      val node_flag : ListBuffer[Int] = ListBuffer()
      val Connected_component_set : Set[Int] = Set()
      queue.enqueue(vertex)

      while(queue.nonEmpty) {
        val vertexId = queue.dequeue()
        Connected_component_set.add(vertexId)
        val leaf_node = graph.apply(vertexId)
        for(node <- leaf_node) {
          if(!node_flag.contains(node)) {
            node_flag.append(node)
            queue.enqueue(node)
          }
        }
      }
      Connected_component_set
    }

    def get_community(graph : Map[Int,List[Int]]) : ListBuffer[Set[Int]] = {
      val node_set = graph.map(_._1).toSet
//      val node_set_total : Set[Int] = Set()
      var node_set_left : Set[Int] = Set()
      for(node <- node_set) {
//        node_set_total.add(node)
        node_set_left.add(node)
      }
      val community : ListBuffer[Set[Int]] = ListBuffer()
      while(node_set_left.nonEmpty) {
        val tmp_set = Connected_component(node_set_left.head, graph)
//        println(tmp_set)
        community.append(tmp_set)
        node_set_left = node_set_left.diff(tmp_set)
//        println(node_set_left)
      }
      community
    }

    def Compute_modularity(community : ListBuffer[Set[Int]], graph : Map[Int,List[Int]]) : Double = {
      val node_pair : Map[(Int, Int), Double] = Map()
      community.foreach(f => {
        for(node1 <- f) {
          for(node2 <- f) {
            if(node1 != node2) {
              if(graph.contains(node1) && graph.apply(node1).contains(node2)) {
                node_pair.update((node1,node2), (1.0 - (graph.apply(node1).size*graph.apply(node2).size).toDouble/graph.values.map(f => f.size).sum))
              }
              else {
                node_pair.update((node1,node2), (0.0 - (graph.apply(node1).size*graph.apply(node2).size).toDouble/graph.values.map(f => f.size).sum))
              }
            }
//            println(node_pair)
          }
        }
      })
      val modularity = node_pair.values.sum/graph.values.map(f => f.size).sum
      modularity
    }

//    val g = Map(1-> List(2,3), 2-> List(1,3,4), 3-> List(1,2), 4-> List(2,5,6,7), 5-> List(4,6), 6-> List(4,5,7), 7-> List(4,6))
    val betweenness = Compute_betweenness(graph_mutable)
    val output1 = sc.makeRDD(betweenness.toSeq)
    output1.map(f => (f._1._1, f._1._2, f._2)).sortBy(f => f._2).sortBy(f => f._1).repartition(1).saveAsTextFile("Chenyu_Peng_betweenness.txt")

    var betweenness_sort_reverse = betweenness.toList.map(f => (f._2,f._1)).sorted.reverse
    val betweenness_sort = betweenness.toList.map(f => (f._2,f._1)).sorted
    var betweenness_max = betweenness_sort_reverse(0)._1
    val betweenness_min = betweenness_sort(0)._1

    while(betweenness_max >= betweenness_min) {
//      println("111111111111111")
      val community = get_community(graph_mutable)
//      println("222222222222222")
      val modularity = Compute_modularity(community, graph_mutable)
      println(modularity)
      val edge_delete : ListBuffer[(Int,Int)] = ListBuffer()
      betweenness_sort_reverse.foreach(f => {
        if(f._1 == betweenness_max) {
          edge_delete.append(f._2)
        }
      })
      edge_delete.foreach(f => {
        graph_mutable.update(f._1, graph_mutable.apply(f._1).filter(s => s != f._2))
        betweenness.remove((f._1,f._2))
      })
      betweenness_sort_reverse = betweenness.toList.map(f => (f._2,f._1)).sorted.reverse
      betweenness_max = betweenness_sort_reverse(0)._1
    }

    val output2 = sc.makeRDD(betweenness.toSeq)
    output2.map(f => f._1).repartition(1).saveAsTextFile("Chenyu_Peng_communities.txt")
  }
}
