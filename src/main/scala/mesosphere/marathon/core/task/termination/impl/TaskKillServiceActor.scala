package mesosphere.marathon.core.task.termination.impl

import akka.actor.{ Actor, ActorLogging, Props }
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.task.{ Task, TaskStateOp }
import mesosphere.marathon.core.task.tracker.{ TaskStateOpProcessor, TaskTracker }
import mesosphere.marathon.event.MesosStatusUpdateEvent

import scala.collection.mutable
import scala.concurrent.Promise

// TODO: kill in batches
// TODO: retry kills
private[impl] class TaskKillServiceActor(
    taskTracker: TaskTracker,
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor) extends Actor with ActorLogging {
  import TaskKillServiceActor._

  private[this] val tasksToKill: mutable.HashMap[Task.Id, Option[Task]] = mutable.HashMap.empty

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[MesosStatusUpdateEvent])
  }

  override def receive: Receive = {
    case KillTaskById(taskId) =>
      killTaskById(taskId)

    case KillTask(task) =>
      killTask(task)

    case KillUnknownTaskById(taskId) =>
      killUnknownTaskById(taskId)

    case KillTasks(tasks, promise) =>
      killTasks(tasks, promise)

    case Terminal(event) if tasksToKill.contains(event.taskId) =>
      handleTerminal(event.taskId)

    case unhandled: Request =>
      log.warning("Received unhandled {}", unhandled)

    case unexpected: Any =>
      log.debug("Received unexpected {}", unexpected)
  }

  def killTaskById(taskId: Task.Id): Unit = {
    log.debug("Received KillTaskById({})", taskId)
    import context.dispatcher
    taskTracker.task(taskId).map {
      case Some(task) => self ! KillTask(task)
      case _ => self ! KillUnknownTaskById(taskId)
    }
  }

  def killTask(task: Task): Unit = {
    log.debug("Received KillTask({})", task)
    tasksToKill.update(task.taskId, Some(task))
    processKills()
  }

  def killUnknownTaskById(taskId: Task.Id): Unit = {
    log.debug("Received KillUnknownTaskById({})", taskId)
    tasksToKill.update(taskId, None)
    processKills()
  }

  def killTasks(tasks: Iterable[Task], promise: Promise[Unit]): Unit = {
    log.debug("Adding {} tasks to queue; setting up child actor to track progress", tasks.size)
    // TODO: watch children? Fail promises in postStop?
    // TODO (urgent): this is hard to test. Add a factory that can create mocks in a test, or use a trait to talk to
    context.actorOf(TaskKillProgressActor.props(tasks.map(_.taskId), promise))
    tasks.foreach { task =>
      tasksToKill.update(task.taskId, Some(task))
    }
    processKills()
  }

  private def processKills(): Unit = {
    tasksToKill.foreach {
      case (taskId, Some(task)) if isLost(task) =>
        log.warning("Expunging lost task {} from state because it should be killed", taskId)
        // TODO: should we map into the future and handle the result?
        stateOpProcessor.process(TaskStateOp.ForceExpunge(taskId))
      // we will eventually be notified of a taskStatusUpdate after the task has been expunged

      case (taskId, None) =>
        log.warning("Killing unknown {}", taskId)
        driverHolder.driver.map(_.killTask(taskId.mesosTaskId))

      case (taskId, _) =>
        log.warning("Killing {}", taskId)
        // TODO: evaluate the status?
        driverHolder.driver.map(_.killTask(taskId.mesosTaskId))
    }
  }

  private def handleTerminal(taskId: Task.Id): Unit = {
    tasksToKill.remove(taskId)
    log.info(s"$taskId is terminal. Waiting for ${tasksToKill.size} more tasks to be killed.")
  }

  private def isLost(task: Task): Boolean = {
    import org.apache.mesos
    task.mesosStatus.fold(false)(_.getState == mesos.Protos.TaskState.TASK_LOST)
  }
}

private[termination] object TaskKillServiceActor {

  sealed trait Request
  case class KillTask(task: Task) extends Request
  case class KillTasks(tasks: Iterable[Task], promise: Promise[Unit]) extends Request
  case class KillTaskById(taskId: Task.Id) extends Request
  case class KillUnknownTaskById(taskId: Task.Id) extends Request

  //  case class TaskKillStatus(taskId: Task.Id, task: Option[Task], lastKillRequest: Timestamp)

  def props(
    taskTracker: TaskTracker,
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor): Props = Props(
    new TaskKillServiceActor(taskTracker, driverHolder, stateOpProcessor))
}
