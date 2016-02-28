package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.Cancellable

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
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class ReplicatorRemoved(replicator: ActorRef)

  // Debugging messages
  case class GetReplicatorInfo()
  case class ReplicatorInfo(count: Int, replicators: Set[ActorRef])

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import Retryer._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  // maintained for efficiency since insert and remove messages (which are very common) would otherwise require a call to secondaries.values.toSet
  var replicators = Set.empty[ActorRef]

  // tracks which client requests an operation (value) and associated ResponseTracker (key)
  var requesters = Map.empty[ActorRef, ActorRef]

  // Tracks next sequence number for secondary replicas
  var nextSequence = 0

  var persistor = context.actorOf(persistenceProps)

  // Negative id range for use when sending data to a new replicator
  var nid = -1

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /**
    * Creates a new replicator to handle messages to and from the replica
    * Called only by the primary replica when a new secondary replica is added to the system
    * @param replica the new secondary replica
    */
  def addReplica(replica: ActorRef) = {
    val newReplicator = context.actorOf(Replicator.props(replica), "replicator")
    secondaries = secondaries updated(replica, newReplicator)
    replicators = replicators + newReplicator

    // Send existing stored values to new replicator
    // A negative id is used to distinguish initializing messages from normal messages
    kv foreach { case (key, value) =>
      newReplicator ! Replicate(key, Some(value), nid)
      nid = nid - 1
    }
  }

  /**
    * Called only by the primary replica when a new secondary replica is removed the system
    * @param replica the new secondary replica
    */
  def removeReplica(replica: ActorRef): Unit = {
    // Notify any outstanding responseTrackers that a replicator has been removed
    requesters.keys.foreach(responseTracker => responseTracker ! ReplicatorRemoved(secondaries(replica)))
    // Remove the replica and its associated replicator from the maintained lists
    replicators = replicators - secondaries(replica)
    secondaries = secondaries - replica
  }


  // Behavior for  the primary replica role.
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = kv updated(key, value)
      // Persist and Replicate messages are sent out by a ResponseTracker actor. It keeps track of the responses and sends
      //  back either an OperationAck message or an OperationFailed message
      val responseTracker = context.actorOf(ResponseTracker.props(id, replicators, Replicate(key, Some(value), id), persistor, Persist(key, Some(value), id)))
      requesters = requesters updated(responseTracker, sender())
    }

    case Remove(key, id) => {
      kv = kv - key
      // Persist and Replicate messages are sent out by a ResponseTracker actor. It keeps track of the responses and sends
      //  back either an OperationAck message or an OperationFailed message
      val responseTracker = context.actorOf(ResponseTracker.props(id, replicators, Replicate(key, None, id), persistor, Persist(key, None, id)))
      requesters = requesters updated(responseTracker, sender())
    }

    case Get(key, id) => {
      context.sender() ! GetResult(key, kv get key, id)
    }

    case OperationAck(id) => {
      val responseTracker = sender()
      requesters(responseTracker) ! OperationAck(id)
      requesters = requesters - responseTracker
    }

    case OperationFailed (id) => {
      val responseTracker = sender()
      requesters(responseTracker) ! OperationFailed(id)
      requesters = requesters - responseTracker
    }

    // Indicates that either a secondary replica was added or removed and needs to be handled
    case Replicas(replicas) => {
      // Tracks the old secondary replicas to determine if a replica was removed
      var oldReplicas = secondaries.keySet

      replicas foreach { replica =>
        // The primary replica is included in the message but requires no action
        if (replica != self) {
          oldReplicas -= replica

          // Case: New secondary replica
          if (!(secondaries contains replica)) addReplica(replica)
        }
      }

      // Case: Removed secondary replica
      // Any remaining oldReplicas have been removed
      oldReplicas foreach { oldReplica => removeReplica(oldReplica) }
    }

    // should only happen when sending data to a new Replicator, can be ignored
    case Replicated(key, id) =>
  }

  // Behavior for the secondary replica role.
  val replica: Receive = {
    case Get(key, id) => context.sender() ! GetResult(key, kv get(key), id)

    case Snapshot(key, valueOption, seq) => {
      if (!(secondaries contains self)) secondaries = secondaries updated(self, sender())
      if (seq == nextSequence) {
        valueOption match {
          // Insert case
          case Some(value) => kv = kv updated(key, value)
          // Remove case
          case None => kv = kv - key
        }

        nextSequence += 1
        // Send and retry persist message
        context.actorOf(Retryer.props(Persist(key, valueOption, seq), persistor))
      }

      // Message already handled
      else if (seq < nextSequence) context.sender() ! SnapshotAck(key, seq)
    }

    case Persisted(key, seq) => secondaries(self) ! SnapshotAck(key, seq)

    // Do nothing as per the assignment specification, the Replicator will timeout on its own
    case PersistFailed(id) =>
  }

  // Initiates the process to join the cluster
  arbiter ! Join

}

