package keyvaluestore

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import keyvaluestore.Mediator.{Replicas, _}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.util.Random

object Replica {

  sealed trait Operation {
    def key: String
    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailure(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(mediator: ActorRef, persistenceProps: Props): Props = 
    Props(new Replica(mediator, persistenceProps))
}

class Replica(val mediator: ActorRef, persistenceProps: Props) extends Actor {

  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  
  // Persistence operations
  // local persistence object
  lazy val persistence: ActorRef = context.actorOf(persistenceProps)
  // Map of seq to (Sender, Cancellable Task) for persistence operations
  private var persistTasks = Map.empty[Long, (ActorRef, Cancellable)]

  // Map of id to Set of (Sender, Cancellable Task) for replicate operations 
  private var replicateTasks = Map.empty[Long, Set[(ActorRef, Cancellable)]]
  // Operation buffer
  private var operationBuffer = Map.empty[Long, (ActorRef, Cancellable)]
  // Map of unacked snapshot sequence number to their requesters 
  private var unackedSnapshotRequesters = Map.empty[Long, ActorRef]
  // Expected sequence number of Replicator's Snapshot Seq (relevant for Secondary Replica only) 
  private var expectedSeq: Long = 0L

  // Send a Join message to Mediator
  mediator ! Join

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /** *************************************************************
    *
    * Primary replica
    *
    * *************************************************************/

  val leader: Receive = {
    case op: Operation => handleOperation(op)
    case Replicas(replicaSet) => handleNewReplicaSet(replicaSet)
    case Replicated(_, id) => handleReplicateMessage(id)
    case Persisted(_, id) => {
      persistTasks(id)._2.cancel()
      persistTasks -= id
      updateIfOperationDone(id)
    }
  }

  def handleOperation(op: Operation) = {
    op match {
      case Get(key, id) => {
        sender ! GetResult(key, kv.get(key), id)  
      }
      case Insert(key, value, id) => {
        scheduleForTimeout(id)
        kv += key -> value
        scheduleForPersistence(key, Some(value), id)
        scheduleForReplication(key, Some(value), id)
      }
      case Remove(key, id) => {
        scheduleForTimeout(id)
        kv -= key
        scheduleForPersistence(key, None, id)
        scheduleForReplication(key, None, id)
      }
    }
  }

  def handleReplicateMessage(id: Long) = {
    if (replicateTasks.contains(id)) {
      val replicateTaskSet = replicateTasks(id)
      for ((requester, cancellableTask) <- replicateTaskSet) {
        if (requester == sender) {
          cancellableTask.cancel()
        }
      }
      val remainingTaskSet = replicateTaskSet.filter { case (requester, _) => requester != sender }
      
      if (remainingTaskSet.isEmpty) {
        replicateTasks -= id
      } else {
        replicateTasks += id -> remainingTaskSet
      }
    }
    
    updateIfOperationDone(id)
  }

  def handleNewReplicaSet(replicaSet: Set[ActorRef]) = {
    removeOldReplicas(replicaSet)
    addNewReplicas(replicaSet)
    for ((key, value) <- kv) {
      scheduleForReplication(key, Some(value), Random.nextLong)
    }
  }

  def removeOldReplicas(replicaSet: Set[ActorRef]) = {
    val removedReplicas = secondaries.keySet -- replicaSet
    for (replica <- removedReplicas) {
      var newReplicateTasks = Map.empty[Long, Set[(ActorRef, Cancellable)]]
      var relevantIds = Set.empty[Long]
      for ((id, remainingTaskSet) <- replicateTasks) {
        remainingTaskSet foreach {
          case (requester, cancellableTask) if requester == secondaries(replica) => {
            cancellableTask.cancel()
          }
        }
        val newTaskSet = remainingTaskSet.filter { case (requester, _) => requester != secondaries(replica) }
        if (newTaskSet.isEmpty) {
          relevantIds += id
        } else {
          newReplicateTasks += id -> newTaskSet
        }
      }
      replicateTasks = newReplicateTasks
      relevantIds foreach { updateIfOperationDone(_) }
      secondaries(replica) ! PoisonPill
      replicators -= replica
      secondaries -= replica
    }
  }

  def addNewReplicas(replicaSet: Set[ActorRef]) = {
    val addedReplicas = (replicaSet - self) -- secondaries.keySet 
    for (replica <- addedReplicas) {
      val replicator = context.actorOf(Replicator.props(replica))
      secondaries += replica -> replicator
      replicators += replicator
    }
  }

  /** *************************************************************
    *
    * Secondary replicas
    *
    * *************************************************************/

  val replica: Receive = {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id) 
    }
    case Snapshot(key, valueOption, seq) => handleSnapshot(key, valueOption, seq)
    case Persisted(key, id) => handlePersisted(key, id)
  }
  
  def handleSnapshot(key: String, valueOption: Option[String], seq: Long) {
    if (seq == expectedSeq) {
      valueOption match {
        case Some(value) => kv += key -> value
        case None => kv -= key
      }
      expectedSeq += 1  
      unackedSnapshotRequesters += seq -> sender
      scheduleForPersistence(key, valueOption, seq)        
    } else {
      if (seq < expectedSeq) {
        // If sequence number smaller than expected, immediately ACK
        sender ! SnapshotAck(key, seq)
      }
      // If sequence number greater than expected, ignore
    }
  }

  def handlePersisted(key: String, id: Long) {
    // After receiving persisted ACK, cancel the associated persist task
    if (persistTasks.contains(id)) {
      persistTasks(id)._2.cancel()
      persistTasks -= id
    }
    // And send SnapshotAck to Replicator
    if (unackedSnapshotRequesters.contains(id)) {
      unackedSnapshotRequesters(id) ! SnapshotAck(key, id)
      unackedSnapshotRequesters -= id
    }
  }

  /** *************************************************************
    *
    * Supervision of persistence nodes
    *
    * *************************************************************/

  /**
    * Handling persistence failures by means of restarting the corresponding actor
    */

  // Make sure to watch the persistence node as it might die
  override def supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }

  /** *************************************************************
    *
    * Useful auxiliary methods for performing timed operations
    *
    * *************************************************************/

  /**
    * Schedule a snapshot for persistence
    *
    * @param k a key 
    * @param vo and optional value
    * @param id identified of the request
    */
  // Hint: use scheduler.cancel() method to cancel the scheduling 
  private def scheduleForPersistence(k: String, vo: Option[String], id: Long) {
    val scheduler = context.system.scheduler
    val cancellableTask =
      scheduler.schedule(Duration.Zero, 100 millis, persistence, Persist(k, vo, id))
    persistTasks = persistTasks + (id -> (sender, cancellableTask))
  }

  private def scheduleForReplication(key: String, valueOption: Option[String], id: Long) = {
    if (!replicators.isEmpty) {
      var replicateTaskSet = Set.empty[(ActorRef, Cancellable)]
      val replicateMessage = Replicate(key, valueOption, id)
      for (replicator <- replicators) {
        val scheduler = context.system.scheduler
        val cancellableTask = scheduler.schedule(Duration.Zero, 100 millis, replicator, replicateMessage)
        replicateTaskSet += ((replicator, cancellableTask))
      }
      replicateTasks += id -> replicateTaskSet
    }
  }

  private def scheduleForTimeout(id: Long) = {
    val scheduler = context.system.scheduler
    val operationFailure = OperationFailure(id)
    val cancellableTask = scheduler.scheduleOnce(1.second, sender, operationFailure)
    operationBuffer += id -> (sender, cancellableTask)
  }

  def updateIfOperationDone(id: Long) = {
    if (operationBuffer.contains(id) && !(persistTasks.contains(id) || replicateTasks.contains(id))) {
      val (requester, cancellableTask) = operationBuffer(id)
      cancellableTask.cancel()
      operationBuffer -= id
      requester ! OperationAck(id)
    }
  }

}
