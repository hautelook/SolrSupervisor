package solr.supervisor

import org.apache.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.WatchedEvent
import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import ZooKeeper.Record
import scala.collection.JavaConversions._

/**
  * Created by Ahmed Khanzada on 1/7/16.
  */
class CollectionActor extends Actor with LazyLogging {
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

case class LiveNodeWatcher(deadNode: ActorRef) extends CuratorWatcher with LazyLogging {
  override def process(event: WatchedEvent): Unit = {
    logger.info(event.toString)
    Thread.sleep(5000l)

    deadNode ! GetChildren(event.getPath)
  }
}

case class GetChildren(path: String)
