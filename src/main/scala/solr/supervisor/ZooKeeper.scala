package solr.supervisor

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import scala.collection.JavaConversions._
import net.liftweb.json._

/**
  * Created by Ahmed Khanzada on 1/7/16.
  */
object ZooKeeper {
  val PathLiveNodes = "/live_nodes"
  val PathCollections = "/collections"
  val PathState = "/collections/%s/state.json"

  case class Record(node: String, core: String, shard: String, collection: String, replicationFactor: Int)

  lazy val client = {
    val baseSleepTime = 1000
    val maxRetries = 3
    val retryPolicy = new ExponentialBackoffRetry(baseSleepTime, maxRetries)

    val client = CuratorFrameworkFactory.builder()
      .connectString(Config.hostZooKeeper)
      .retryPolicy(retryPolicy)
      .build()

    client.start()
    client
  }

  // blocking
  def liveNodes = client.getChildren.forPath(PathLiveNodes).toSet

  def collectionsState = client.getChildren.forPath(PathCollections)

  def stateBytes(collection: String) = client.getData.forPath(PathState.format(collection))

  def stateJson = stateBytes _ andThen (new String(_)) andThen parse

  def stateMap(collection: String) = collectionMap(collection, stateJson(collection))

  def stateFlatMap = collectionsState.flatMap(stateMap)

  def transformReplica(field: JField) = {
    val core = field.name
    val JString(node) = field \ "node_name"

    node -> core
  }

  def getReplicas(s: JField) = JsonExtractor.extractReplicas(s).map(
    transformReplica _ andThen (x => (x._1, x._2, s.name))
  )

  def collectionMap(collection: String, json: JValue) = {
    val replicationFactor = JsonExtractor.extractReplicationFactor(collection, json)

    JsonExtractor.extractShards(collection, json).flatMap(getReplicas).map( replica =>
      Record(
        node = replica._1,
        core = replica._2,
        shard = replica._3,
        collection = collection,
        replicationFactor = replicationFactor
      )
    )
  }
}

object JsonExtractor {
  val KeyReplication = "replicationFactor"
  val KeyShards = "shards"
  val KeyReplicas = "replicas"

  def extractReplicationFactor(collection: String, json: JValue) = {
    val JString(replicationFactor) = extract(json, collection, KeyReplication)
    replicationFactor.toInt
  }

  def extractShards(collection: String, json: JValue) = {
    val JObject(shards) = extract(json, collection, KeyShards)
    shards
  }

  def extractReplicas(shard: JField) = {
    val JObject(replicas) = shard \ KeyReplicas
    replicas
  }

  private def extract(json: JValue, collection: String, key: String) = json \ collection \ key
}
