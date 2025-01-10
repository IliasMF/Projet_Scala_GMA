import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger, PropertyConfigurator}

object SparkGraphXExample {
  def main(args: Array[String]): Unit = {
    // Configurer le logging pour rediriger les logs vers un fichier
    PropertyConfigurator.configure("src/main/resources/log4j.properties")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Initialiser SparkContext
    val conf = new SparkConf()
      .setAppName("GroupesAmis")
      .setMaster("local")
    val sc = new SparkContext(conf)

    // Créer les nœuds (amis)
    val amis: RDD[(VertexId, String)] = sc.parallelize(Array(
      (1L, "Mohamed"), // Mohamed
      (2L, "Alice"),   // Alice
      (3L, "Asma"),    // Asma
      (4L, "Tom"),     // Tom
      (5L, "Ayman"),   // Ayman
      (6L, "Sarah")    // Sarah
    ))

    // Créer les arêtes (relations d'amitié)
    val relations: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(1L, 2L, "amis"),  // Mohamed et Alice sont amis
      Edge(2L, 3L, "amis"),  // Alice et Asma sont amies
      Edge(4L, 5L, "amis"),  // Tom et Ayman sont amis
      Edge(5L, 6L, "amis")   // Ayman et Sarah sont amis
    ))

    // Créer le graphe
    val graph = Graph(amis, relations)

    // Utiliser l'algorithme Connected Components pour détecter les groupes d'amis
    val groupes = graph.connectedComponents()

    // Afficher les groupes d'amis
    println("Groupes d'amis :")
    groupes.vertices.collect().foreach { case (id, groupeId) =>
      println(s"${amis.lookup(id).head} fait partie du groupe $groupeId")
    }

    // Arrêter SparkContext
    sc.stop()
  }
}
