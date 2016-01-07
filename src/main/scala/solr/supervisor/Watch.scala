package solr.supervisor

import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.duration._

/**
 * Created by ren on 8/31/15.
 */
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
