package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props}
import kvstore.Arbiter._

import scala.concurrent.duration._

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

  case class PersistTimeout(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Persistence._
  import Replica._
  import Replicator._
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

  var persistAcks = Map.empty[Long, ActorRef]
  var replicateAcks = Map.empty[Long, Set[ActorRef]]
  var retryTasks = Map.empty[Long, Cancellable]
  var failedTasks = Map.empty[Long, Cancellable]

  val getReceive: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def retryTask(key: String, valueOption: Option[String], id: Long) =
    system.scheduler.schedule(0.seconds, 100.milliseconds, persistence, Persist(key, valueOption, id))

  def failedTask(id: Long) = system.scheduler.scheduleOnce(1.second, self, PersistTimeout(id))

  def persist(key: String, valueOpt: Option[String], id: Long): Unit = {
    println("persist id = " + id)
    persistAcks += id -> sender()

    val retry = retryTask(key, valueOpt, id)
    retryTasks += id -> retry

    val failedScheduler = failedTask(id)
    failedTasks += id -> failedScheduler

    if (replicators.nonEmpty) {
      replicators foreach { repl => repl ! Replicate(key, valueOpt, id) }
      replicateAcks += id -> Set(replicators.toArray:_*)
    }
  }

  def checkPersist(id: Long): Unit = {
    if (retryTasks.get(id).isEmpty && replicateAcks.get(id).isEmpty) {
      failedTasks.get(id) foreach (_.cancel())
      failedTasks -= id

      persistAcks.get(id) foreach (_ ! OperationAck(id))
      persistAcks -= id
    }
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = getReceive orElse {
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      persist(key, Option(value), id)
    case Remove(key, id) =>
      kv = kv - key
      persist(key, None, id)
    case Replicas(replicas) =>
      val allSecondary = replicas.filterNot(_ == self)
      val newSecondary = allSecondary -- secondaries.keySet
      val secondaryToDelete = secondaries.keySet -- allSecondary

      secondaryToDelete foreach { repl =>
        repl ! PoisonPill

        val replicator = secondaries(repl)
        secondaries(repl) ! PoisonPill
        secondaries = secondaries - repl

        val ids = replicateAcks.keySet

        replicateAcks = replicateAcks.map { case (id, set) =>
          id -> (set - replicator)
        }.filter{case (id, set) => set.nonEmpty}

        ids.foreach(checkPersist)
      }

      newSecondary foreach { secondary =>
        val replicator = system.actorOf(Props(classOf[Replicator], secondary))
        replicators += replicator
        secondaries += (secondary -> replicator)
        kv.zipWithIndex foreach { case ((key, value), id) =>
          replicator ! Replicate(key, Option(value), id)
        }
      }

    case Persisted(key, id) =>
      retryTasks.get(id) foreach (_.cancel())
      retryTasks -= id
      checkPersist(id)
    case Replicated(key, id) =>
      if (replicateAcks.contains(id)){
        val newSet = replicateAcks(id) - sender()
        if(newSet.isEmpty) {
          replicateAcks -= id
          checkPersist(id)
        } else {
          replicateAcks += id -> newSet
        }
      }
    case PersistTimeout(id) =>
      retryTasks.get(id) foreach (_.cancel())
      retryTasks -= id
      failedTasks -= id
      replicateAcks -= id

      val clOpt = persistAcks.get(id)
      persistAcks -= id

      clOpt foreach(_ ! OperationFailed(id))

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

