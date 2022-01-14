package keyvaluestore

import akka.actor._
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to snapshot information, which includes (sender, request, scheduled task to cancel)
  var snapshotInfo = Map.empty[Long, (ActorRef, Replicate, Cancellable)]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* Behavior for the Replicator. */
  def receive: Receive = {
    case replicateMessage @ Replicate(key, valueOption, _) => {
      val currentSeq = nextSeq
      val snapshot = Snapshot(key, valueOption, currentSeq)
      val scheduler = context.system.scheduler
      val cancellableTask: Cancellable = scheduler.schedule(Duration.Zero, 100 millis, replica, snapshot)
      snapshotInfo += currentSeq -> (sender, replicateMessage, cancellableTask)
    }

    case SnapshotAck(key, seq) => {
      if (snapshotInfo.contains(seq)) {
        val (requester, request, cancellableTask) = snapshotInfo(seq)
        requester ! Replicated(key, request.id)
        cancellableTask.cancel()
        snapshotInfo -= seq
      }
    }
  }

}
