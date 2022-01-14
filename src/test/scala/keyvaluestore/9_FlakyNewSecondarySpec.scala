package keyvaluestore

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import keyvaluestore.Mediator._
import keyvaluestore.Replicator._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class Step9_NewSecondarySpec extends TestKit(ActorSystem("Step9FlakyNewSecondarySpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with Tools {

  override def afterAll(): Unit = {
    system.terminate()
  }

  "[09-Flaky-New-Replicas]" must {
    "case 1: Primary must start replication to new replicas" in {
      val mediator = TestProbe()
      val primary = system.actorOf(Replica.props(mediator.ref, Persistence.props(flaky = true)), "case1-primary")
      val user = session(primary)
      val secondary = TestProbe()

      mediator.expectMsg(Join)
      mediator.send(primary, JoinedPrimary)

      user.setAcked("k1", "v1")
      mediator.send(primary, Replicas(Set(primary, secondary.ref)))

      secondary.expectMsg(Snapshot("k1", Some("v1"), 0L))
      secondary.reply(SnapshotAck("k1", 0L))

      val ack1 = user.set("k1", "v2")
      secondary.expectMsg(Snapshot("k1", Some("v2"), 1L))
      secondary.reply(SnapshotAck("k1", 1L))
      user.waitAck(ack1)

      val ack2 = user.remove("k1")
      secondary.expectMsg(Snapshot("k1", None, 2L))
      secondary.reply(SnapshotAck("k1", 2L))
      user.waitAck(ack2)
    }

    "case 2: Primary must stop replication to removed replicas and stop Replicator" in {
      val mediator = TestProbe()
      val primary = system.actorOf(Replica.props(mediator.ref, Persistence.props(flaky = true)), "case2-primary")
      val user = session(primary)
      val secondary = TestProbe()

      mediator.expectMsg(Join)
      mediator.send(primary, JoinedPrimary)
      mediator.send(primary, Replicas(Set(primary, secondary.ref)))

      val ack1 = user.set("k1", "v1")
      secondary.expectMsg(Snapshot("k1", Some("v1"), 0L))
      val replicator = secondary.lastSender
      secondary.reply(SnapshotAck("k1", 0L))
      user.waitAck(ack1)

      watch(replicator)
      mediator.send(primary, Replicas(Set(primary)))
      expectTerminated(replicator)
    }

    "case 3: Primary must stop replication to removed replicas and waive their outstanding acknowledgements" in {
      val mediator = TestProbe()
      val primary = system.actorOf(Replica.props(mediator.ref, Persistence.props(flaky = true)), "case3-primary")
      val user = session(primary)
      val secondary = TestProbe()

      mediator.expectMsg(Join)
      mediator.send(primary, JoinedPrimary)
      mediator.send(primary, Replicas(Set(primary, secondary.ref)))

      val ack1 = user.set("k1", "v1")
      secondary.expectMsg(Snapshot("k1", Some("v1"), 0L))
      secondary.reply(SnapshotAck("k1", 0L))
      user.waitAck(ack1)

      val ack2 = user.set("k1", "v2")
      secondary.expectMsg(Snapshot("k1", Some("v2"), 1L))
      mediator.send(primary, Replicas(Set(primary)))
      user.waitAck(ack2)
    }


  }

}
