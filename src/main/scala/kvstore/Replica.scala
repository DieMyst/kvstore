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

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */


  override def preStart() = {
    arbiter ! Join
  }

  var _seqCounter = 0L
  def incSeq() = {
    _seqCounter += 1
  }

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val getReceive: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = getReceive orElse {

    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      sender ! OperationAck(id)
    case Remove(key, id) =>
      kv = kv - key
      sender ! OperationAck(id)
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = getReceive orElse {
    case Snapshot(key, valueOption, seq) =>
      if (seq == _seqCounter) {
        incSeq()
        valueOption match {
          case Some(value) =>
            kv = kv + (key -> value)
            sender ! SnapshotAck(key, seq)
          case None =>
            kv = kv - key
            sender() ! SnapshotAck(key, seq)
        }
      } else if (seq < _seqCounter) {
        sender ! SnapshotAck(key, seq)
      }
    case _ =>
  }
}

