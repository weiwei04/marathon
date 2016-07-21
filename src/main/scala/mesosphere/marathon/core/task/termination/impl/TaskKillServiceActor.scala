package mesosphere.marathon.core.task.termination.impl

import akka.actor.{ Actor, ActorLogging, Cancellable, Props }
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.task.termination.TaskKillConfig
import mesosphere.marathon.core.task.tracker.{ TaskStateOpProcessor, TaskTracker }
import mesosphere.marathon.core.task.{ Task, TaskStateOp }
import mesosphere.marathon.event.MesosStatusUpdateEvent
import mesosphere.marathon.state.Timestamp

import scala.collection.mutable
import scala.concurrent.Promise

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
    config: TaskKillConfig,
    clock: Clock) extends Actor with ActorLogging {
  import TaskKillServiceActor._
  import context.dispatcher

  val tasksToKill: mutable.HashMap[Task.Id, Option[Task]] = mutable.HashMap.empty
  val inFlight: mutable.HashMap[Task.Id, TaskToKill] = mutable.HashMap.empty
  var retryTimer: Option[Cancellable] = None

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[MesosStatusUpdateEvent])
  }

  //scalastyle:off cyclomatic.complexity
  override def receive: Receive = {
    case KillTaskById(taskId) =>
      killTaskById(taskId)

    case KillTask(task) =>
      killTask(task)

    case KillUnknownTaskById(taskId) =>
      killUnknownTaskById(taskId)

    case KillTasks(tasks, promise) =>
      killTasks(tasks, promise)

    case Terminal(event) if inFlight.contains(event.taskId) || tasksToKill.contains(event.taskId) =>
      handleTerminal(event.taskId)

    case Retry =>
      retry()

    case unhandled: Request =>
      log.warning("Received unhandled {}", unhandled)

    case unexpected: Any =>
      log.debug("Received unexpected {}", unexpected)
  }
  //scalastyle:on

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
    context.actorOf(TaskKillProgressActor.props(tasks.map(_.taskId), promise))
    tasks.foreach { task =>
      tasksToKill.update(task.taskId, Some(task))
    }
    processKills()
  }

  private def processKills(): Unit = {
    val killCount = config.killChunkSize - inFlight.size
    val toKillNow = tasksToKill.take(killCount)

    log.info("processing {} kills", toKillNow.size)
    toKillNow.foreach {
      case (taskId, maybeTask) => processKill(taskId, maybeTask)
    }

    retryTimer = Some(context.system.scheduler.scheduleOnce(config.killRetryTimeout, self, Retry))
  }

  def processKill(taskId: Task.Id, maybeTask: Option[Task]): Unit = {
    val taskIsLost: Boolean = maybeTask.fold(false)(isLost)

    if (taskIsLost) {
      log.warning("Expunging lost {} from state because it should be killed", taskId)
      // we will eventually be notified of a taskStatusUpdate after the task has been expunged
      stateOpProcessor.process(TaskStateOp.ForceExpunge(taskId))
    } else {
      val knownOrNot = if (maybeTask.isDefined) "known" else "unknown"
      log.warning("Killing {} {}", knownOrNot, taskId)
      driverHolder.driver.map(_.killTask(taskId.mesosTaskId))
    }

    val attempts = inFlight.get(taskId).fold(1)(_.attempts + 1)
    inFlight.update(taskId, TaskToKill(taskId, maybeTask, issued = clock.now(), attempts))
    tasksToKill.remove(taskId)
  }

  def handleTerminal(taskId: Task.Id): Unit = {
    tasksToKill.remove(taskId)
    inFlight.remove(taskId)
    log.debug("{} is terminal. ({} kills queued, {} in flight)", taskId, tasksToKill.size, inFlight.size)
    processKills()
  }

  def retry(): Unit = {
    val now = clock.now()

    inFlight.foreach {
      case (taskId, taskToKill) if taskToKill.attempts >= config.killRetryMax =>
        log.warning("Expunging {} from state: max retries reached", taskId)
        stateOpProcessor.process(TaskStateOp.ForceExpunge(taskId))

      case (taskId, taskToKill) if (taskToKill.issued + config.killRetryTimeout) < now =>
        log.warning("No kill ack received for {}, retrying", taskId)
        processKill(taskId, taskToKill.maybeTask)

      case _ => // ignore
    }

    if (inFlight.nonEmpty) {
      retryTimer = Some(context.system.scheduler.scheduleOnce(config.killRetryTimeout, self, Retry))
    } else {
      retryTimer = None
    }
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

  sealed trait InternalRequest
  case object Retry extends InternalRequest

  case class TaskToKill(taskId: Task.Id, maybeTask: Option[Task], issued: Timestamp, attempts: Int)

  def props(
    taskTracker: TaskTracker,
    driverHolder: MarathonSchedulerDriverHolder,
    stateOpProcessor: TaskStateOpProcessor,
    config: TaskKillConfig,
    clock: Clock): Props = Props(
    new TaskKillServiceActor(taskTracker, driverHolder, stateOpProcessor, config, clock))
}
