package solr.supervisor

/**
  * Created by Ahmed Khanzada on 1/7/16.
  */
object Config {
  val LogSignal = "Received %s, terminating"
  val LogCardinality = "Shard cardinality: %s"
  val LogSpareNodes = "Spare nodes as seen by %s: %s"
  val LogRequest = "GET %s:\n%s"

  val DefaultHostZooKeeper = "127.0.0.1:2181/solr"
  val DefaultHostSolr = "127.0.0.1:8983"

  lazy val SystemHostZooKeeper = System.getProperty("solr.zk")
  lazy val SystemHostSolr = System.getProperty("solr.host")

  lazy val hostZooKeeper = Option(SystemHostZooKeeper).getOrElse(DefaultHostZooKeeper)
  lazy val hostSolr = Option(SystemHostSolr).getOrElse(DefaultHostSolr)
}
