import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXPageRank {
  def main(args: Array[String]): Unit = {

    // Start Spark
    val spark = SparkSession.builder()
      .appName("GraphX PageRank")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // 1. Read the people: (id, name). Skip the header line
    val users: RDD[(VertexId, String)] =
      sc.textFile("data/vertices.csv")
        .filter(!_.startsWith("id,"))
        .map { line =>
          val f = line.split(",")
          (f(0).toLong, f(1))
        }

    // 2. Read the follows: Edge(from, to, weight 1)
    val follows: RDD[Edge[Int]] =
      sc.textFile("data/edges.csv")
        .filter(!_.startsWith("src,"))
        .map { line =>
          val f = line.split(",")
          Edge(f(0).toLong, f(1).toLong, 1)
        }

    // 3. Build the graph
    val graph = Graph(users, follows)
    println("Total Users: " + graph.numVertices)
    println("Total Relationships: " + graph.numEdges)

    // 4. Run PageRank: gives (id, score)
    val ranks = graph.pageRank(0.0001).vertices

    // 5. Join scores with names and sort, highest first
    val result = users.join(ranks)
      .map { case (id, (name, rank)) => (name, rank) }
      .sortBy(_._2, ascending = false)

    println("PageRank Results")
    result.collect().foreach { case (name, rank) =>
      println(name + " : " + rank)
    }

    println("Most Influential User: " + result.first()._1)

    spark.stop()
  }
}
