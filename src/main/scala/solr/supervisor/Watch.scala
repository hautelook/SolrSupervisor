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
  lazy val solrZk = Option(System.getProperty("solr.zk")).getOrElse("127.0.0.1:2181/solr")
  lazy val solrHost = Option(System.getProperty("solr.host")).getOrElse("127.0.0.1:8983")
}

case class CollectionsApi(solrHost: String, collection: String) {
  def addReplica(shard: String) =
    s"""http://$solrHost/solr/admin/collections?action=ADDREPLICA&collection=$collection&shard=$shard"""
  def deleteReplica(shard: String, core: String) =
    s"""http://$solrHost/solr/admin/collections?action=DELETEREPLICA&collection=$collection&shard=$shard&replica=$core"""
  def add(shard: String)(implicit r: RequestUrl): Unit = r.request(addReplica(shard))
  def delete(shard: String, core: String)(implicit r: RequestUrl): Unit = r.request(deleteReplica(shard, core))
}

trait RequestUrl extends LazyLogging {
  def request(url: String): String
}
object PrintCurlCommand extends RequestUrl {
  def request(url: String) =  {
    val curl = s"""curl "$url""""
    logger.info(curl)
    curl
  }
}
object UrlUtils extends RequestUrl {
  def request(url: String) =  {
    val output = scala.io.Source.fromURL(url).mkString
    logger.info(s"GET $url:\n$output")
    output
  }
}

case class Capi(collection: String) {
  implicit val requester = UrlUtils
  //implicit val requester = PrintCurlCommand
  def add = CollectionsApi(Config.solrHost, collection).add _
  def delete = CollectionsApi(Config.solrHost, collection).delete _
}

object ZK {
  lazy val client = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.builder().connectString(Config.solrZk).retryPolicy(retryPolicy).build()
    client.start()
    client
  }

  case class Record(node: String, core: String, shard: String, collection: String, replicationFactor: Int)

  // blocking
  def liveNodes = client.getChildren.forPath("/live_nodes").toSet
  def collections = client.getChildren.forPath("/collections")
  def stateBytes(collection: String) = client.getData.forPath(s"/collections/$collection/state.json")
  def stateJson = stateBytes _ andThen (new String(_)) andThen parse

  def statem(c: String) = collectionMap(c, stateJson(c))
  def stateMap = collections.flatMap(statem)

  def extractRepFactor(collection: String, j: JValue) = {
    val JString(rf) = j \ collection \ "replicationFactor"
    rf.toInt
  }
  def extractShards(collection: String, j: JValue) = {
    val JObject(shards) = j \ collection \ "shards"
    shards
  }
  def extractReplicas(shard: JField) = {
    val JObject(replicas) = shard \ "replicas"
    replicas
  }
  def transformReplica(field: JField) = {
    val core = field.name
    val JString(node) = field \ "node_name"
    node -> core
  }
  def getReplicas(s: JField) = extractReplicas(s).map(transformReplica _ andThen (x => (x._1, x._2, s.name)))
  def collectionMap(collection: String, json: JValue) = {
    val rf = extractRepFactor(collection, json)
    extractShards(collection, json).flatMap(getReplicas).map(x => Record(x._1, x._2, x._3, collection, rf))
  }
}

case class GetChildren(path: String)

case class LiveNodeWatcher(deadNode: ActorRef) extends CuratorWatcher with LazyLogging {
  override def process(event: WatchedEvent): Unit = {
    logger.info(event.toString)
    Thread.sleep(5000l) // been getting error 500s, maybe going too fast?
    deadNode ! GetChildren(event.getPath)
  }
}

class CollectionsActor extends Actor with LazyLogging {
  import ZK.Record
  case class Reap(liveNodes: Set[String], nodeMap: Map[String, Seq[Record]])
  case class Sow(liveNodes: Set[String], index: Map[String, Map[String, Seq[Record]]], collections: List[String])

  def receive = {
    case Reap(liveNodes, nodeMap) => reap(liveNodes, nodeMap)
    case Sow(liveNodes, index, collections) => collections.foreach(sow(liveNodes, index, _))
    case GetChildren(path) =>
      val liveNodes = ZK.client.getChildren.usingWatcher(LiveNodeWatcher(self)).forPath(path).toSet
      val state = ZK.stateMap
      val collectionIndexed = state.groupBy(_.collection).mapValues(_.groupBy(_.node))

      // these will queue so even if we get updated too quickly, the Reaps and Sows will stay in the same order
      self ! Reap(liveNodes, state.groupBy(_.node))
      self ! Sow(liveNodes, collectionIndexed, ZK.collections.toList)
  }

  // this should be safe to run multiple times over and over
  def reap(liveNodes: Set[String], nodeMap: Map[String, Seq[Record]]) = {
    val deadNodes = nodeMap.filterKeys(k => !liveNodes.contains(k)).values.flatten
    deadNodes.foreach({ case Record(_, core, shard, coll, _) => Capi(coll).delete(shard, core) })
  }

  def sow(liveNodes: Set[String], index: Map[String, Map[String, Seq[Record]]], collectionName: String) = {
    for (collection <- index.get(collectionName)) {
      // get cardinality of each shard
      val shardCards = collection.filterKeys(liveNodes.contains).values.flatten
        .groupBy(_.shard).mapValues(_.size).toSeq
      logger.info(s"shard cardinalities: $shardCards")

      // spare nodes are live nodes not in the collection
      val spareNodes = liveNodes.filter(n => !collection.contains(n))
      logger.info(s"spare nodes as seen by $collectionName: $spareNodes")

      // only use up to the designated replication factor
      //val replicationFactor = collection.values.flatten.head._5
      //val priority = priorityShards(shardCards, replicationFactor).take(spareNodes.size)

      // use all available nodes for replicas
      val priority = greedyAllNodes(shardCards).take(spareNodes.size)

      // for now we want these to block so that this actor cannot
      priority.foreach(Capi(collectionName).add)
    }
  }

  // prioritize the shards with fewer replicas
  def greedyAllNodes(shards: Seq[(String, Int)]) = {
    val max = shards.maxBy(_._2)._2
    val priority = priorityShards(shards, max)
    priority.toStream ++ Stream.continually(shards).flatten.map(_._1)
  }

  // prioritize the shards with fewer replicas
  def priorityShards(shards: Seq[(String, Int)], replicationFactor: Int) = {
    shards.groupBy(_._2).toSeq.sortBy(_._1)
      .flatMap(x => List.fill(replicationFactor - x._1)(x._2).flatten).map(_._1)
  }
}

object Watch extends App with LazyLogging {
  val system = ActorSystem("live_nodes")

  import system.dispatcher
  val scheduled = system.scheduler.schedule(0.milliseconds, 10.minutes,
    system.actorOf(Props[CollectionsActor]), GetChildren("/live_nodes"))

  val cleanup = new SignalHandler {
    override def handle(signal: Signal): Unit = {
      logger.info(s"got $signal signal, cleaning up and terminating...")
      scheduled.cancel()
      ZK.client.close()
      system.shutdown()
    }
  }

  Signal.handle(new Signal("INT"), cleanup)
  Signal.handle(new Signal("TERM"), cleanup)

  system.awaitTermination()
}
