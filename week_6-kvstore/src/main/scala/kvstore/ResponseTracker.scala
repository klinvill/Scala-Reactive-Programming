package kvstore

import akka.actor.{Props, Actor, ActorRef}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import kvstore.Persistence.{Persisted, Persist}
import kvstore.Replica.{ReplicatorRemoved, OperationFailed, OperationAck}
import kvstore.Replicator.{Replicated, Replicate}
import kvstore.ResponseTracker.{timeout}


/**
  * Created by Kirby on 2/8/16.
  */
object ResponseTracker {
  case object timeout

  def props(id: Long, replicators: Set[ActorRef], replicateMessage: Replicate, persistor: ActorRef, persistMessage: Persist) =
    Props(new ResponseTracker(id, replicators, replicateMessage, persistor, persistMessage))
}

/**
  * Actor created for a single insert or remove message that's responsible for tracking acknowledgement messages from
  *   the replicators and persistor. Sends out an OperationAck message if all actors respond before the timeout,
  *   otherwise sends out an OperationFailed message.
  * @param id Operation id
  * @param replicators Set of replicators to expect response from
  * @param replicateMessage replicate message to send to replicators
  * @param persistor persistence actor to expect response from
  * @param persistMessage persist message to send to persistor
  */
class ResponseTracker(id: Long, replicators: Set[ActorRef], replicateMessage: Replicate, persistor: ActorRef,
                      persistMessage: Persist) extends Actor {
  // Persistence can fail so we need to create a retryer
  val persistRetryer = context.actorOf(Retryer.props(persistMessage, persistor))

  var notifiers: Set[ActorRef] = replicators + persistRetryer

  replicators foreach { replicator => replicator ! replicateMessage }

  context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, timeout)

  /**
    * Removes the actor from the notifiers list and sends out an OperationAck if all expected responses have been received
    * @param actor an ActorRef for either a Replicator or a Persistor
    */
  def recordResponse(actor: ActorRef) = {
    if (notifiers contains actor) notifiers = notifiers - actor
    if (notifiers.isEmpty) {
      context.parent ! OperationAck(id)
      context.stop(self)
    }
  }

  def receive = {
    case msg: Persisted => recordResponse(sender())
    case msg: Replicated => recordResponse(sender())
    case ReplicatorRemoved(replicator) => recordResponse(replicator)

    case timeout =>
      context.parent ! OperationFailed(id)
      context.stop(self)
  }
}
