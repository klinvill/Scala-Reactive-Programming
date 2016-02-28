package kvstore

import akka.actor.{ReceiveTimeout, Props, ActorRef, Actor}
import scala.concurrent.ExecutionContext.Implicits.global
import kvstore.Persistence.{PersistFailed, Persisted, Persist}
import kvstore.Replicator.{Snapshot, SnapshotFailed, SnapshotAck}

import scala.concurrent.duration._

/**
  * Created by Kirby on 2/8/16.
  */

object Retryer {
  def props(message: Any, target: ActorRef) = Props(new Retryer(message, target))
}

/**
  * Actor that handles periodically re-sending messages until a response or timeout is received
  * Useful for when actors or communication may fail, in our case persistence and communicating with replicas are
  *  simulated points of failure (for writing to disks and communicating across a network)
  * @param message message to periodically re-send
  * @param target actor to send messages to
  */
class Retryer(message: Any, target: ActorRef) extends Actor {
  val retryInterval = Duration(100, MILLISECONDS)
  val timeoutDuration = Duration(1, SECONDS)

  // Message received indicating success
  var successfulMessage: Any = null
  // Messages to send on success or failure
  var successfulResponse: Any = null
  var failedResponse: Any = null

  def receive = message match {
    case message: Snapshot => replicaRetryer
    case message: Persist => persistRetryer

    case _ => throw new NotImplementedError("Retriable message %s is not defined in Retryer.scala".format(message.getClass))
  }

  val scheduledRetry = context.system.scheduler.schedule(Duration.Zero, retryInterval, target, message)

  context.setReceiveTimeout(timeoutDuration)

  def replicaRetryer: Receive = {
    // Pass SnapshotAck back to Replicator
    case SnapshotAck(key, seq) => {
      scheduledRetry.cancel()
      context.parent ! SnapshotAck(key, seq)
      context.stop(self)
    }

    case ReceiveTimeout => {
      scheduledRetry.cancel()
      message match { case message: Snapshot => context.parent ! SnapshotFailed(message.seq) }
      context.stop(self)
    }
  }

  def persistRetryer: Receive = {
    // Pass SnapshotAck back to Replicator
    case Persisted(key, id) => {
      scheduledRetry.cancel()
      context.parent ! Persisted(key, id)
      context.stop(self)
    }

    case ReceiveTimeout => {
      scheduledRetry.cancel()
      message match { case message: Persist => context.parent ! PersistFailed(message.id) }
      context.stop(self)
    }
  }
}
