package mesosphere.marathon.core.task.termination.impl

import akka.actor.{Actor, ActorLogging, Props}
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.task.tracker.{TaskStateOpProcessor, TaskTracker}
import mesosphere.marathon.core.task.{Task, TaskStateOp}
import mesosphere.marathon.event.MesosStatusUpdateEvent
import mesosphere.marathon.upgrade.UpgradeConfig

import scala.collection.mutable
import scala.concurrent.Promise

// TODO: retry kills
/**
  * An actor that handles killing tasks in chunks and depending on the task state.
  * Lost tasks will simply be expunged from state, while active tasks will be killed
  * via the scheduler driver. There is be a maximum number of kills in flight, and
  * the service will only issue more kills when tasks are reported terminal.
  */
private[impl] class TaskKillServiceActor(
    taskTracker: TaskTracker,
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor,
    config: UpgradeConfig) extends Actor with ActorLogging {
  import TaskKillServiceActor._

  val tasksToKill: mutable.HashMap[Task.Id, Option[Task]] = mutable.HashMap.empty
  val inFlight: mutable.HashMap[Task.Id, Option[Task]] = mutable.HashMap.empty
  val chunkSize: Int = config.killBatchSize

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
    context.actorOf(TaskKillProgressActor.props(tasks.map(_.taskId), promise))
    tasks.foreach { task =>
      tasksToKill.update(task.taskId, Some(task))
    }
    processKills()
  }

  private def processKills(): Unit = {
    val killCount = chunkSize - inFlight.size
    val toKillNow = tasksToKill.take(killCount)
    log.info("chunkSize: {}, inFlight: {}, killCount: {}, toKillNow.size: {}", chunkSize, inFlight.size, killCount, tasksToKill.size)

    log.info("processing {} kills", toKillNow.size)
    toKillNow.foreach {
      case (taskId, maybeTask) => processKill(taskId, maybeTask)
    }
  }

  def processKill(taskId: Task.Id, maybeTask: Option[Task]): Unit = {
    val taskIsLost: Boolean = maybeTask.fold(false)(isLost)

    if (taskIsLost) {
      log.warning("Expunging lost task {} from state because it should be killed", taskId)
      // TODO: should we map into the future and handle the result?
      // we will eventually be notified of a taskStatusUpdate after the task has been expunged
      stateOpProcessor.process(TaskStateOp.ForceExpunge(taskId))
    } else {
      val knownOrNot = if (maybeTask.isDefined) "known" else "unknown"
      log.warning("Killing {} {}", knownOrNot, taskId)
      driverHolder.driver.map(_.killTask(taskId.mesosTaskId))
    }

    inFlight.update(taskId, maybeTask)
    tasksToKill.remove(taskId)
  }

  def handleTerminal(taskId: Task.Id): Unit = {
    tasksToKill.remove(taskId)
    inFlight.remove(taskId)
    log.debug("{} is terminal. ({} kills queued, {} in flight)", taskId, tasksToKill.size, inFlight.size)
    processKills()
  }

  def isLost(task: Task): Boolean = {
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

  def props(
    taskTracker: TaskTracker,
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor,
    config: UpgradeConfig): Props = Props(
    new TaskKillServiceActor(taskTracker, driverHolder, stateOpProcessor, config))
}
