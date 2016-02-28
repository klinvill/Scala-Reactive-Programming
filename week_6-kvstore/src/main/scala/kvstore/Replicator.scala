package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class SnapshotFailed(seq: Long)

  case object GetNumAcks
  case class NumAcks(numAcks: Int)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case message: Replicate => {
      val seq = nextSeq
      val msg = Snapshot(message.key, message.valueOption, seq)
      acks = acks updated (seq, (context.sender(), message))
      context.actorOf(Retryer.props(msg, replica))
    }

    case SnapshotAck(key, seq) => {
      acks get seq match {
        case Some((primaryNode, message)) => {
          primaryNode ! Replicated(message.key, message.id)
          acks = acks - seq
        }
        case None =>
      }
    }

    case SnapshotFailed(seq) => {
      acks get seq match {
        case Some((primaryNode, message)) => {
          acks = acks - seq
        }
        case None =>
      }
    }

    // Used for debugging and testing
    case GetNumAcks => sender() ! NumAcks(acks.size)
  }

}
