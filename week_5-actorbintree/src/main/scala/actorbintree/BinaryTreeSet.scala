/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Prints out tree for debugging */
  case object PrintTree

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC => {
      val newRoot = createRoot
      printf("\n"); newRoot ! PrintNode(0)
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)
    }
    case PrintTree => { printf("\n"); root ! PrintNode(0) }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC =>
    case op: Operation => pendingQueue = pendingQueue enqueue op
    case CopyFinished => {
      root = newRoot
      context become normal
      pendingQueue.foreach(op => root ! op)
      pendingQueue = Queue.empty[Operation]
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  case class PrintNode(level: Int)

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  val GCOpId = -1

  // optional
  def receive = normal

  def addNode(element: Int, pos: Position) = { subtrees = subtrees.updated(pos, context.actorOf(props(element, false))) }

  def finishCopy = { context.parent ! CopyFinished; context.stop(self)}

  def printNode(value: Int, deleted: Boolean, level: Int) = printf ("(val: %d, deleted: %b, level: %d), ", value, deleted, level)
  def printEmptyNode(level: Int) = printf ("(val: empty, level: %d), ", level)

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case Insert(requester, id, element) => {
      if (elem == element) {
        if (removed) { removed = false; requester ! OperationFinished(id) }
        else requester ! OperationFinished(id)
      }
      else if (elem < element) {
        if (subtrees contains Left) subtrees(Left) ! Insert(requester, id, element)
        else { addNode(element, Left); requester ! OperationFinished(id) }
      }
      else if (elem > element) {
        if (subtrees contains Right) subtrees(Right) ! Insert(requester, id, element)
        else { addNode(element, Right); requester ! OperationFinished(id) }
      }
    }

    case Contains(requester, id, element) => {
      if (elem == element) {
        if (removed) requester ! ContainsResult(id, false)
        else requester ! ContainsResult(id, true)
      }
      else if (elem < element) {
        if (subtrees contains Left) subtrees(Left) ! Contains(requester, id, element)
        else requester ! ContainsResult(id, false)
      }
      else if (elem > element) {
        if (subtrees contains Right) subtrees(Right) ! Contains(requester, id, element)
        else requester ! ContainsResult(id, false)
      }
    }

    case Remove(requester, id, element) => {
      if (elem == element) {
        if (removed) requester ! OperationFinished(id)
        else { removed = true; requester ! OperationFinished(id) }
      }
      else if (elem < element) {
        if (subtrees contains Left) subtrees(Left) ! Remove(requester, id, element)
        else requester ! OperationFinished(id)
      }
      else if (elem > element) {
        if (subtrees contains Right) subtrees(Right) ! Remove(requester, id, element)
        else requester ! OperationFinished(id)
      }
    }

    case CopyTo(newRoot) => {
      if (removed && subtrees.isEmpty) finishCopy

      else {
        if (!removed) newRoot ! Insert(self, GCOpId, elem)
        subtrees.values.foreach(actor => actor ! CopyTo(newRoot))
        context become copying(subtrees.values.toSet, removed)
      }
    }

    case PrintNode(level) => {
      if (subtrees contains Left) subtrees(Left) ! PrintNode(level+1)
      else printEmptyNode(level+1)

      printNode(elem, removed, level)

      if (subtrees contains Right) subtrees(Right) ! PrintNode(level+1)
      else printEmptyNode(level+1)
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(GCOpId) => {
      if (expected.isEmpty) finishCopy
      else context become copying(expected, true)
    }
    case CopyFinished => {
      // TODO: newExpected could throw an error if the sender isn't in expected
      val newExpected = expected - sender()
      if (newExpected.isEmpty && insertConfirmed) finishCopy
      else context become copying(newExpected, insertConfirmed)
    }
  }


}
