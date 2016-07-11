package mesosphere.marathon.core.task.update.impl.steps

import javax.inject.Named

import akka.event.EventStream
import com.google.inject.Inject
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.task.bus.MarathonTaskStatus.WithMesosStatus
import mesosphere.marathon.core.task.bus.TaskChangeObservables.TaskChanged
import mesosphere.marathon.core.task.update.TaskUpdateStep
import mesosphere.marathon.core.task.{ EffectiveTaskStateChange, Task, TaskStateOp }
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

    // TODO: write unit tests to verify that ALL expunges/terminations are posted to the event stream and do
    // obviously denote a task as terminal

    taskChanged match {
      // case 1: Mesos status update => update or expunge
      // In this case, we post the NEW state
      case TaskChanged(MesosUpdate(_, WithMesosStatus(status), now), EffectiveTaskStateChange(task)) =>
        postEvent(clock.now(), Some(status), task)

      // case 2: Any TaskStateOp => update or expunge
      // In this case, we post the NEW state
      case TaskChanged(_, EffectiveTaskStateChange(task)) =>
        postEvent(clock.now(), task.mesosStatus, task)

      case _ =>
        log.debug("Ignoring noop for {}", taskChanged.taskId)
    }

    Future.successful(())
  }

  private[this] def postEvent(timestamp: Timestamp, maybeStatus: Option[TaskStatus], task: Task): Unit = {
    val taskId = task.taskId

    val ports = task.launched.fold(Seq.empty[Int])(_.hostPorts)
    val version = task.launched.fold("n/a")(_.runSpecVersion.toString)

    for {
      status <- maybeStatus
    } {
      log.info(
        "Sending event notification for {} of app [{}]: {}",
        Array[Object](taskId, taskId.runSpecId, status.getState): _*
      )
      eventBus.publish(
        MesosStatusUpdateEvent(
          slaveId = status.getSlaveId.getValue,
          taskId = Task.Id(status.getTaskId),
          taskStatus = status.getState.name,
          message = if (status.hasMessage) status.getMessage else "",
          appId = taskId.runSpecId,
          host = task.agentInfo.host,
          ipAddresses = Task.MesosStatus.ipAddresses(status),
          ports = ports,
          version = version,
          timestamp = timestamp.toString
        )
      )

    }
  }

}
