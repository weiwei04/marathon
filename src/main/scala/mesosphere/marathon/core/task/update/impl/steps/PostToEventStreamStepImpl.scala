package mesosphere.marathon.core.task.update.impl.steps

import javax.inject.Named

import akka.event.EventStream
import com.google.inject.Inject
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.task.bus.MarathonTaskStatus.{ Terminal, WithMesosStatus }
import mesosphere.marathon.core.task.bus.TaskChangeObservables.TaskChanged
import mesosphere.marathon.core.task.update.TaskUpdateStep
import mesosphere.marathon.core.task.{ EffectiveTaskStateChange, Task, TaskStateChange, TaskStateOp }
import mesosphere.marathon.event.{ EventModule, MesosStatusUpdateEvent }
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.TaskStatus
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.collection.immutable.Seq

/**
  * Post this update to the internal event stream.
  */
class PostToEventStreamStepImpl @Inject() (
    @Named(EventModule.busName) eventBus: EventStream, clock: Clock) extends TaskUpdateStep {

  private[this] val log = LoggerFactory.getLogger(getClass)

  override def name: String = "postTaskStatusEvent"

  override def processUpdate(taskChanged: TaskChanged): Future[_] = {
    import TaskStateOp.MesosUpdate
    val taskState = inferTaskState(taskChanged)

    taskChanged match {
      // the task was updated or expunged due to a MesosStatusUpdate
      // In this case, we're interested in the mesosStatus
      case TaskChanged(MesosUpdate(oldTask, WithMesosStatus(status), now), EffectiveTaskStateChange(task)) =>
        postEvent(clock.now(), taskState, Some(status), task, inferVersion(task, Some(oldTask)))

      case TaskChanged(_, TaskStateChange.Update(newState, oldState)) =>
        postEvent(clock.now(), taskState, newState.mesosStatus, newState, inferVersion(newState, oldState))

      case TaskChanged(_, TaskStateChange.Expunge(task)) =>
        postEvent(clock.now(), taskState, task.mesosStatus, task, inferVersion(task, None))

      case _ =>
        log.debug("Ignoring noop for {}", taskChanged.taskId)
    }

    Future.successful(())
  }

  // inconvenient for now because not all tasks have a version
  private[this] def inferVersion(newTask: Task, oldTask: Option[Task]): Timestamp = {
    newTask.version.getOrElse(oldTask.fold(Timestamp(0))(_.version.getOrElse(Timestamp(0))))
  }

  private[this] def inferTaskState(taskChanged: TaskChanged): String = {
    (taskChanged.stateOp, taskChanged.stateChange) match {
      // TODO: A terminal MesosStatusUpdate for a resident transitions to state Reserved
      case (TaskStateOp.MesosUpdate(_, Terminal(status), _), TaskStateChange.Update(newState, oldState)) =>
        status.mesosStatus.fold(MesosStatusUpdateEvent.OtherTerminalState)(_.getState.toString)
      case (TaskStateOp.MesosUpdate(_, WithMesosStatus(mesosStatus), _), _) => mesosStatus.getState.toString
      case (_, TaskStateChange.Expunge(task)) => MesosStatusUpdateEvent.OtherTerminalState
      case (_, TaskStateChange.Update(newState, maybeOldState)) => MesosStatusUpdateEvent.Created
    }
  }

  private[this] def postEvent(
    timestamp: Timestamp,
    taskStatus: String,
    maybeStatus: Option[TaskStatus],
    task: Task,
    version: Timestamp): Unit = {

    val taskId = task.taskId
    val slaveId = maybeStatus.fold("n/a")(_.getSlaveId.getValue)
    val message = maybeStatus.fold("")(status => if (status.hasMessage) status.getMessage else "")
    val host = task.agentInfo.host
    val ipAddresses = maybeStatus.flatMap(status => Task.MesosStatus.ipAddresses(status))
    val ports = task.launched.fold(Seq.empty[Int])(_.hostPorts)

    log.info("Sending event notification for {} of app [{}]: {}", taskId, taskId.runSpecId, taskStatus)
    eventBus.publish(
      MesosStatusUpdateEvent(
        slaveId,
        taskId,
        taskStatus,
        message,
        appId = taskId.runSpecId,
        host,
        ipAddresses,
        ports = ports,
        version = version.toString,
        timestamp = timestamp.toString
      )
    )
  }

}
