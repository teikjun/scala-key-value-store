package keyvaluestore

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import keyvaluestore.Mediator.{Join, JoinedPrimary, Replicas}
import keyvaluestore.Persistence.{Persist, Persisted}
import keyvaluestore.Replicator.{Snapshot, SnapshotAck}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class Step8_PrimaryPersistenceSpec extends TestKit(ActorSystem("Step8FlakyPrimaryPersistenceSpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with Tools {

  override def afterAll(): Unit = {
    system.terminate()
  }
  
  "[08-Flaky-Primary-Persistence]" must {
    
    "case 1: Primary generates failure after 1 second if global acknowledgement fails" in {
      val mediator = TestProbe()
      val persistence = TestProbe()
      val primary = system.actorOf(Replica.props(mediator.ref, Persistence.props(flaky = true)), "case4-primary")
      val secondary = TestProbe()
      val client = session(primary)

      mediator.expectMsg(Join)
      mediator.send(primary, JoinedPrimary)
      mediator.send(primary, Replicas(Set(primary, secondary.ref)))

      val setId = client.set("foo", "bar")
      secondary.expectMsgType[Snapshot]
      client.nothingHappens(800.milliseconds) // Should not fail too early
      client.waitFailed(setId)
    }

    "case 2: Primary acknowledges only after persistence and global acknowledgement" in {
      val mediator = TestProbe()
      val persistence = TestProbe()
      val primary = system.actorOf(Replica.props(mediator.ref, Persistence.props(flaky = true)), "case5-primary")
      val secondaryA, secondaryB = TestProbe()
      val client = session(primary)

      mediator.expectMsg(Join)
      mediator.send(primary, JoinedPrimary)
      mediator.send(primary, Replicas(Set(primary, secondaryA.ref, secondaryB.ref)))

      val setId = client.set("foo", "bar")
      val seqA = secondaryA.expectMsgType[Snapshot].seq
      val seqB = secondaryB.expectMsgType[Snapshot].seq
      client.nothingHappens(300.milliseconds)
      secondaryA.reply(SnapshotAck("foo", seqA))
      client.nothingHappens(300.milliseconds)
      secondaryB.reply(SnapshotAck("foo", seqB))
      client.waitAck(setId)
    }

  }

}