package mesosphere.marathon.core.task.termination.impl

import akka.actor.{ Actor, ActorLogging, Props }
import mesosphere.marathon.KillingTasksFailedException
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.event.MesosStatusUpdateEvent

import scala.concurrent.Promise
import scala.collection.mutable
import scala.util.Try

/**
  * Actor implementation that watches over a given set of taskIds and completes a promise when all
  * tasks have been reported terminal (including LOST et al)
  *
  * @param taskIds the taskIds that shall be watched.
  * @param promise the promise that shall be completed when all tasks have been reported terminal.
  */
class TaskKillProgressActor(taskIds: mutable.HashSet[Task.Id], promise: Promise[Unit]) extends Actor with ActorLogging {
  // TODO: should we timeout at some point? Retrying kills should be covered by the TaskKillService.
  // TODO: if one of the watched task is reported terminal before this actor subscribed to the event bus,
  //       it won't receive that event. should we reconcile tasks after a certain amount of time?

  // this should be used for logging to prevent polluting the logs
  val name = "TaskKillProgressActor" + self.hashCode()

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[MesosStatusUpdateEvent])
    log.info("Starting {} to track kill progress of {} tasks", name, taskIds.size)
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)

    if (!promise.isCompleted) {
      val msg = s"$name was stopped before all tasks are killed. Outstanding: ${taskIds.mkString(",")}"
      log.error(msg)
      promise.failure(new KillingTasksFailedException(msg))
    }
  }

  override def receive: Receive = {
    case Terminal(event) if taskIds.contains(event.taskId) =>
      log.debug("Received terminal update for {}", event.taskId)
      taskIds.remove(event.taskId)
      if (taskIds.isEmpty) {
        log.info("All tasks watched by {} are killed, completing promise", name)
        val success = promise.tryComplete(Try(()))
        if (!success) log.error("Promise has already been completed in {}", name)
        context.stop(self)
      } else {
        log.info("{} still waiting for {} tasks to be killed", name, taskIds.size)
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
