/**
 * Copyright (C) 2015-2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster.sharding

import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.sharding.ShardCoordinator.Internal.ShardRegionRegistered
import akka.persistence.{ PersistentActor, Persistence }
import akka.testkit.{ TestProbe, AkkaSpec }
import akka.testkit.TestActors.EchoActor
import scala.concurrent.duration._

object ShardCoordinatorStartFailureSpec {
  val config = """
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.remote.netty.tcp.port = 0
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.journal.inmem {
    }
                 """
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int ⇒ (msg.toString, msg)
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int ⇒ (msg % 10).toString
  }
}

class FailPersistenceActor extends PersistentActor {
  def dummy = () ⇒ ()
  override def persistenceId = "/sharding/type1Coordinator"

  override def receiveRecover: Receive = {
    case rr ⇒ println(s"RR ${rr}")
  }

  override def receiveCommand: Receive = {
    case x: ShardRegionRegistered ⇒
      persist(x) { event ⇒ }
  }

}

class ShardCoordinatorStartFailureSpec extends AkkaSpec(ShardCoordinatorStartFailureSpec.config) {
  import ShardCoordinatorStartFailureSpec._
  "ShardCoordinator failing to start" must {
    "notify system.EventStream about it's failure" in {
      //prepare broken state - persiste 2 same allocations
      val evt = ShardRegionRegistered(TestProbe().ref)
      val breakingActor = system.actorOf(Props[FailPersistenceActor])
      breakingActor ! evt
      breakingActor ! evt

      val probe = TestProbe()
      system.eventStream.subscribe(probe.ref, classOf[ShardCoordinatorStartFailure])
      Cluster(system).join(Cluster(system).selfAddress)
      val settings = ClusterShardingSettings(system)
      ClusterSharding(system).start("type1", Props[EchoActor], settings, extractEntityId, extractShardId)
      val msg = probe.expectMsgType[ShardCoordinatorStartFailure](100 millis)
      msg.typeName should ===("type1")

    }
  }

}
