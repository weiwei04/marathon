package mesosphere.marathon.core.task.termination.impl

import akka.actor.ActorRef
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.TaskKillService

import scala.concurrent.{ Future, Promise }

private[impl] class TaskKillServiceDelegate(actorRef: ActorRef) extends TaskKillService {
  import TaskKillServiceActor._

  override def kill(tasks: Iterable[Task]): Future[Unit] = {
    val promise = Promise[Unit]
    actorRef ! KillTasks(tasks, promise)
    promise.future
  }

  override def kill(taskId: Task.Id): Unit = {
    actorRef ! KillTaskById(taskId)
  }

  override def kill(task: Task): Unit = {
    actorRef ! KillTask(task)
  }

}
