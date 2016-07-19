package mesosphere.marathon.core.task

import akka.actor.ActorSystem
import mesosphere.marathon.core.task.Task.Id
import mesosphere.marathon.core.task.termination.TaskKillService
import mesosphere.marathon.event.MesosStatusUpdateEvent
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future

/**
  * A Mocked TaskKillService that publishes a TASK_KILLED event for each given task and always works successfully
  */
class TaskKillServiceMock(system: ActorSystem) extends TaskKillService {

  private[this] val log = LoggerFactory.getLogger(getClass)
  var numKilled = 0
  val customStatusUpdates = mutable.Map.empty[Task.Id, MesosStatusUpdateEvent]
  val killed = mutable.Set.empty[Task.Id]

  override def kill(tasks: Iterable[Task]): Future[Unit] = {
    tasks.foreach { task =>
      kill(task.taskId)
    }
    Future.successful(())
  }
  override def kill(taskId: Task.Id): Unit = {
    val appId = taskId.runSpecId
    val update = customStatusUpdates.getOrElse(taskId, MesosStatusUpdateEvent("", taskId, "TASK_KILLED", "", appId, "", None, Nil, "no-version"))
    system.eventStream.publish(update)
    numKilled += 1
    killed += taskId
  }

  override def kill(task: Task): Unit = kill(task.taskId)

  override def killUnknownTask(taskId: Id): Unit = kill(taskId)
}

