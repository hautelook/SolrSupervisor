package solr.supervisor

import com.typesafe.scalalogging.LazyLogging

/**
  * Created by Ahmed Khanzada on 1/7/16.
  */
case class CollectionCommander(collection: String) {
  implicit val requester = UrlUtils

  def add = CollectionApi(Config.hostSolr, collection).add _
  def delete = CollectionApi(Config.hostSolr, collection).delete _
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
