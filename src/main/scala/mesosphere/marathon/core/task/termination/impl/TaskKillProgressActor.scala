package mesosphere.marathon.core.task.termination.impl

import akka.actor.{ Actor, ActorLogging, Props }
import mesosphere.marathon.KillingTasksFailedException
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.event.MesosStatusUpdateEvent

import scala.concurrent.Promise
import scala.collection.mutable
import scala.util.Try

// TODO: should we timeout at some point? Retrying kills should be covered by the TaskKillService.
class TaskKillProgressActor(taskIds: mutable.HashSet[Task.Id], promise: Promise[Unit]) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[MesosStatusUpdateEvent])
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)

    if (!promise.isCompleted) {
      val msg = s"$self was stopped before all tasks are killed. Outstanding: ${taskIds.mkString(",")}"
      log.error(msg)
      promise.failure(new KillingTasksFailedException(msg))
    }
  }

  override def receive: Receive = {
    case Terminal(event) if taskIds.contains(event.taskId) =>
      taskIds.remove(event.taskId)
      if (taskIds.isEmpty) {
        val alreadyCompleted = promise.tryComplete(Try(Unit))
        if (alreadyCompleted) log.error("Promise has already been completed in {}", self)
        context.stop(self)
      }

    case _ => // ignore
  }
}

object TaskKillProgressActor {
  def props(toKill: Iterable[Task.Id], promise: Promise[Unit]): Props = {
    val taskIds = mutable.HashSet[Task.Id](toKill.toVector: _*)
    Props(new TaskKillProgressActor(taskIds, promise))
  }
}
