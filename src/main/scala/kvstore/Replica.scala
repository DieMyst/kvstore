package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor, Cancellable }
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

  val system = context.system
  val persistence = system.actorOf(persistenceProps)

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

  def retryTask(key: String, valueOption: Option[String], id: Long) = system.scheduler.schedule(0.seconds, 100.milliseconds) {
    persistence ! Persist(key, valueOption, id)
  }

  def failedTask(id: Long) = system.scheduler.scheduleOnce(1.second) {
    self ! OperationFailed(id)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = getReceive orElse {
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      val retry = retryTask(key, Option(value), id)
      val failedScheduler = failedTask(id)
      replicators foreach {repl => repl ! Replicate(key, Option(value), id)}
      context.become(leaderAwaitPersistance(sender(), retry, failedScheduler, replicators, false))
    case Remove(key, id) =>
      kv = kv - key
      val retry = retryTask(key, None, id)
      val failedScheduler = failedTask(id)
      replicators foreach {repl => repl ! Replicate(key, None, id)}
      context.become(leaderAwaitPersistance(sender(), retry, failedScheduler, replicators, false))
    case Replicas(replicas) =>
      replicas.filterNot(ref => ref.equals(self)) foreach { secondary =>
        secondaries.get(secondary) match {
          case None =>
            val replicator = system.actorOf(Props(classOf[Replicator], secondary))
            replicators += replicator
            secondaries += (secondary -> replicator)
          case _ =>
        }
      }
    case m@_ => println(m)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = getReceive orElse {
    case s@Snapshot(key, valueOption, seq) =>
      if (seq == _seqCounter) {
        incSeq()
        valueOption match {
          case Some(value) =>
            kv = kv + (key -> value)
          case None =>
            kv = kv - key
        }
        val retryPersist = system.scheduler.schedule(0.seconds, 100.milliseconds) {
          persistence ! Persist(key, valueOption, seq)
        }
        context.become(replicaAwaitPersistance(s, sender(), retryPersist))
      } else if (seq < _seqCounter) {
        sender ! SnapshotAck(key, seq)
      }

    case _ =>
  }

  def replicaAwaitPersistance(snapshot: Snapshot, repl: ActorRef, retry: Cancellable): Receive = getReceive orElse {
    case Persisted(key, id) =>
      retry.cancel()
      repl ! SnapshotAck(snapshot.key, snapshot.seq)
      context.become(replica)
    case _ =>
  }

  def leaderAwaitPersistance(client: ActorRef, 
                             retryTask: Cancellable, 
                             failedTask: Cancellable,
                             waitSecondaries: Set[ActorRef],
                             isPrimaryPersisted: Boolean): Receive = getReceive orElse {
    case Persisted(key, id) =>
      if (waitSecondaries.isEmpty) {
        retryTask.cancel()
        failedTask.cancel()
        client ! OperationAck(id)
        context.become(leader)
      } else {
        context.become(leaderAwaitPersistance(client, retryTask, failedTask, waitSecondaries, true))
      }
    case Replicated(key, id) =>
      val set = waitSecondaries - sender
      if (set.isEmpty && isPrimaryPersisted) {
        retryTask.cancel()
        failedTask.cancel()
        client ! OperationAck(id)
        context.become(leader)
      } else {
        context.become(leaderAwaitPersistance(client, retryTask, failedTask, set, isPrimaryPersisted))
      }
    case OperationFailed(id) =>
      retryTask.cancel()
      client ! OperationFailed(id)
      context.become(leader)
    case m@_ => println(m)
  }
}

