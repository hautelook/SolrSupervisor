package solr.supervisor

import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import net.liftweb.json._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.WatchedEvent
import sun.misc.{Signal, SignalHandler}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
 * Created by ren on 8/31/15.
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

case class CollectionApi(hostSolr: String, collection: String) {
  val PathCollectionAction = "http://%s/solr/admin/collections?action=%s&collection=%s&shard=%s"
  val PathCollectionActionReplica = "http://%s/solr/admin/collections?action=%s&collection=%s&shard=%s&replica=%s"
  val ActionReplicaAdd = "ADDREPLICA"
  val ActionReplicaDelete = "DELETEREPLICA"

  def add(shard: String)(implicit url: RequestUrl): Unit =
    url.request(addReplica(shard))

  def delete(shard: String, core: String)(implicit url: RequestUrl): Unit =
    url.request(deleteReplica(shard, core))

  def addReplica(shard: String) =
    modifyReplica(ActionReplicaAdd, collection, shard)

  def deleteReplica(shard: String, core: String) =
    modifyReplica(ActionReplicaDelete, collection, shard, Some(core))

  private def modifyReplica(action: String, collection: String, shard: String, core: Option[String] = None) =
    if (core.isEmpty)
      PathCollectionAction.format(hostSolr, action, collection, shard)
    else
      PathCollectionActionReplica.format(hostSolr, action, collection, shard, core)
}

trait RequestUrl extends LazyLogging {
  def request(url: String): String
}

object UrlUtils extends RequestUrl {
  def request(url: String) =  {
    val output = scala.io.Source.fromURL(url).mkString
    logger.info(Config.LogRequest.format(url, output))
    output
  }
}

case class CollectionCommander(collection: String) {
  implicit val requester = UrlUtils

  def add = CollectionApi(Config.hostSolr, collection).add _
  def delete = CollectionApi(Config.hostSolr, collection).delete _
}

object ZooKeeper {
  val PathLiveNodes = "/live_nodes"
  val PathCollections = "/collections"
  val PathState = "/collections/%s/state.json"

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

  case class Record(node: String, core: String, shard: String, collection: String, replicationFactor: Int)

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

case class GetChildren(path: String)

case class LiveNodeWatcher(deadNode: ActorRef) extends CuratorWatcher with LazyLogging {
  override def process(event: WatchedEvent): Unit = {
    logger.info(event.toString)
    Thread.sleep(5000l)

    deadNode ! GetChildren(event.getPath)
  }
}

class CollectionActor extends Actor with LazyLogging {
  import ZK.Record

  case class Reap(liveNodes: Set[String], nodeMap: Map[String, Seq[Record]])

  case class Sow(liveNodes: Set[String], index: Map[String, Map[String, Seq[Record]]], collections: List[String])

  def receive = {
    case Reap(liveNodes, nodeMap) =>
      reap(liveNodes, nodeMap)
    case Sow(liveNodes, index, collections) =>
      collections.foreach(sow(liveNodes, index, _))
    case GetChildren(path) =>
      val liveNodes = ZooKeeper.client.getChildren.usingWatcher(LiveNodeWatcher(self)).forPath(path).toSet
      val state = ZooKeeper.stateFlatMap
      val collectionIndexed = state.groupBy(_.collection).mapValues(_.groupBy(_.node))

      // Will queue and reap and sow in same order
      self ! Reap(liveNodes, state.groupBy(_.node))
      self ! Sow(liveNodes, collectionIndexed, ZooKeeper.collectionsState.toList)
  }

  def reap(liveNodes: Set[String], nodeMap: Map[String, Seq[Record]]) = {
    // Safe to run multiple times
    val deadNodes = nodeMap.filterKeys(key => !liveNodes.contains(key)).values.flatten
    deadNodes.foreach({
      case Record(_, core, shard, collection, _) => CollectionCommander(collection).delete(shard, core)
    })
  }

  def sow(liveNodes: Set[String], index: Map[String, Map[String, Seq[Record]]], collectionName: String) = {
    for (collection <- index.get(collectionName)) {
      val shardCardinalities = collection.filterKeys(liveNodes.contains)
        .values.flatten.groupBy(_.shard).mapValues(_.size).toSeq
      logger.info(Config.LogCardinality.format(shardCardinalities))

      val spareNodes = liveNodes.filter(n => !collection.contains(n))
      logger.info(Config.LogSpareNodes.format(collectionName, spareNodes))

      val priority = greedyAllNodes(shardCardinalities).take(spareNodes.size)
      priority.foreach(
        CollectionCommander(collectionName).add
      )
    }
  }

  private def greedyAllNodes(shards: Seq[(String, Int)]) = {
    val max = shards.maxBy(_._2)._2
    val priority = priorityShards(shards, max)
    priority.toStream ++ Stream.continually(shards).flatten.map(_._1)
  }

  private def priorityShards(shards: Seq[(String, Int)], replicationFactor: Int) = {
    // Prioritize the shards with fewer replicas
    shards.groupBy(_._2).toSeq.sortBy(_._1)
      .flatMap(x => List.fill(replicationFactor - x._1)(x._2).flatten).map(_._1)
  }
}

object Watch extends App with LazyLogging {
  val system = ActorSystem("live_nodes")
  import system.dispatcher

  val scheduled = system.scheduler.schedule(
    initialDelay = 0.milliseconds,
    interval = 10.minutes,
    receiver = system.actorOf(Props[CollectionActor]),
    message = GetChildren(ZooKeeper.PathLiveNodes)
  )

  val cleanup = new SignalHandler {
    override def handle(signal: Signal): Unit = {
      logger.info(Config.LogSignal.format(signal))

      scheduled.cancel()
      ZooKeeper.client.close()
      system.shutdown()
    }
  }

  Signal.handle(new Signal("INT"), cleanup)
  Signal.handle(new Signal("TERM"), cleanup)

  system.awaitTermination()
}
