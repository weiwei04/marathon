package mesosphere.marathon.core.task.termination

import mesosphere.marathon.core.task.Task

import scala.concurrent.Future

// TODO: do we favor distinct vs overloaded functions?
trait TaskKillService {

  /**
    * Kill the given tasks and return a future that is completed when all of the tasks
    * have been reported killed. If the list contains tasks that are lost, the implementation
    * should issue a driver.kill in case the tasks are reachable in the meantime, but should
    * not wait for them to be reported as being killed.
    *
    * @param tasks the tasks that shall be killed.
    * @return a future that is completed when all tasks are killed.
    */
  def kill(tasks: Iterable[Task]): Future[Unit]

  /**
    * Kill the given task. The implementation should add the task onto a queue that is processed
    * short term and will eventually kill the task.
    *
    * @param taskId the id of the task that shall be killed.
    */
  def kill(taskId: Task.Id): Unit

  /**
    * Kill the given task. See [[kill(task: Task.Id)]] The implementation should add the task onto
    * a queue that is processed short term and will eventually kill the task.
    *
    * @param task the task that shall be killed.
    */
  def kill(task: Task): Unit
}
