package mesosphere.marathon.core.task.termination.impl

import mesosphere.marathon.event.MesosStatusUpdateEvent

import scala.util.Try

private[impl] object Terminal {
  import org.apache.mesos.Protos.TaskState
  import org.apache.mesos.Protos.TaskState._

  def unapply(event: MesosStatusUpdateEvent): Option[MesosStatusUpdateEvent] = {
    Try(TaskState.valueOf(event.taskStatus)).map {
      case TASK_ERROR | TASK_FAILED | TASK_KILLED | TASK_FINISHED | TASK_LOST => Some(event)
      case _ => None
    }.getOrElse(None)
  }

}
