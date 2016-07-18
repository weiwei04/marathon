package mesosphere.marathon.core.task.termination.impl

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit, TestProbe }
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.task.termination.TaskKillConfig
import mesosphere.marathon.core.task.tracker.{ TaskStateOpProcessor, TaskTracker }
import mesosphere.marathon.core.task.{ Task, TaskStateOp }
import mesosphere.marathon.event.MesosStatusUpdateEvent
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.test.Mockito
import org.apache.mesos
import org.apache.mesos.SchedulerDriver
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.time.{ Millis, Span }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, GivenWhenThen, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

class TaskKillServiceActorTest extends TestKit(ActorSystem("test"))
    with FunSuiteLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with GivenWhenThen
    with ScalaFutures
    with Matchers
    with Eventually
    with ImplicitSender
    with Mockito {

  test("Kill single known task") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("a single, known running task")
    val task = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)

    When("the service is asked to kill that task")
    actor ! TaskKillServiceActor.KillTask(task)

    Then("a kill is issued to the driver")
    verify(f.driver, timeout(500)).killTask(task.taskId.mesosTaskId)
  }

  test("Kill single known task by ID") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("a single running task")
    val task = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)
    f.taskTracker.task(task.taskId) returns Future.successful(Some(task))

    When("the service is asked to kill that taskId")
    actor ! TaskKillServiceActor.KillTaskById(task.taskId)

    Then("it will fetch the task from the taskTracker")
    verify(f.taskTracker, timeout(500)).task(eq(task.taskId))
    noMoreInteractions(f.taskTracker)

    And("a kill is issued to the driver")
    verify(f.driver, timeout(500)).killTask(task.taskId.mesosTaskId)
    noMoreInteractions(f.driver)
  }

  test("Kill single unknown task by ID") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("an unknown taskId")
    val taskId = Task.Id.forRunSpec(PathId("/unknown"))
    f.taskTracker.task(eq(taskId)) returns Future.successful(None)

    When("the service is asked to kill that taskId")
    actor ! TaskKillServiceActor.KillTaskById(taskId)

    Then("it will fetch the task from the taskTracker")
    verify(f.taskTracker, timeout(500)).task(eq(taskId))
    noMoreInteractions(f.taskTracker)

    And("a kill is issued to the driver")
    verify(f.driver, timeout(500)).killTask(taskId.mesosTaskId)
    noMoreInteractions(f.driver)
  }

  test("Kill single known LOST task") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("a single, known running task")
    val task = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_LOST)

    When("the service is asked to kill that task")
    actor ! TaskKillServiceActor.KillTask(task)

    Then("NO kill is issued to the driver because the task is lost")
    noMoreInteractions(f.driver)

    And("the stateOpProcessor receives an expunge")
    verify(f.stateOpProcessor, timeout(500)).process(TaskStateOp.ForceExpunge(task.taskId))
  }

  test("kill multiple tasks at once (with promise)") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("a list of tasks")
    val runningTask = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)
    val lostTask = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_LOST)
    val stagingTask = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_STAGING)

    When("the service is asked to kill those tasks")
    val promise = Promise[Unit]()
    actor ! TaskKillServiceActor.KillTasks(Seq(runningTask, lostTask, stagingTask), promise)

    Then("the task tracker is not queried")
    noMoreInteractions(f.taskTracker)

    And("three kill requests are issued to the driver")
    verify(f.driver, timeout(500)).killTask(runningTask.taskId.mesosTaskId)
    verify(f.stateOpProcessor, timeout(500)).process(TaskStateOp.ForceExpunge(lostTask.taskId))
    verify(f.driver, timeout(500)).killTask(stagingTask.taskId.mesosTaskId)
    noMoreInteractions(f.driver)

    And("Eventually terminal status updates are published via the event stream")
    f.publishStatusUpdate(runningTask, mesos.Protos.TaskState.TASK_KILLED)
    f.publishStatusUpdate(lostTask, mesos.Protos.TaskState.TASK_LOST)
    f.publishStatusUpdate(stagingTask, mesos.Protos.TaskState.TASK_LOST)

    Then("the promise is eventually completed successfully")
    eventually(promise.isCompleted)
    promise.future.futureValue should be (())
  }

  test("kill multiple tasks at once (empty list)") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("an empty list")
    val emptyList = Seq.empty[Task]

    When("the service is asked to kill those tasks")
    val promise = Promise[Unit]()
    actor ! TaskKillServiceActor.KillTasks(emptyList, promise)

    Then("the promise is eventually completed successfully")
    eventually(promise.isCompleted)
    promise.future.futureValue should be (())

    And("the task tracker is not queried")
    noMoreInteractions(f.taskTracker)

    And("no kill is issued")
    noMoreInteractions(f.driver)
  }

  test("kill multiple tasks subsequently (without promise)") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("multiple tasks")
    val task1 = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)
    val task2 = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)
    val task3 = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)

    When("the service is asked subsequently to kill those tasks")
    actor ! TaskKillServiceActor.KillTask(task1)
    actor ! TaskKillServiceActor.KillTask(task2)
    actor ! TaskKillServiceActor.KillTask(task3)

    Then("exactly 3 kills are issued to the driver")
    verify(f.driver, timeout(500)).killTask(task1.taskId.mesosTaskId)
    verify(f.driver, timeout(500)).killTask(task2.taskId.mesosTaskId)
    verify(f.driver, timeout(500)).killTask(task3.taskId.mesosTaskId)
    noMoreInteractions(f.driver)
  }

  test("killing tasks is throttled (single requests)") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("multiple tasks")
    val tasks = (1 to 10).map { index =>
      f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)
    }

    When("the service is asked to kill those tasks")
    tasks.foreach { task =>
      actor ! TaskKillServiceActor.KillTask(task)
    }

    Then("5 kills are issued immediately to the driver")
    verify(f.driver, times(5)).killTask(any)

    And("after receiving remaining terminal messages, all the tasks are killed eventually")
    tasks.foreach { task =>
      f.publishStatusUpdate(task, mesos.Protos.TaskState.TASK_KILLED)
    }

    verify(f.driver, times(5)).killTask(any)
    noMoreInteractions(f.driver)
  }

  test("killing tasks is throttled (batch request)") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("multiple tasks")
    val tasks = (1 to 10).map { index =>
      f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)
    }

    When("the service is asked to kill those tasks")
    val promise = Promise[Unit]()
    actor ! TaskKillServiceActor.KillTasks(tasks, promise)

    Then("5 kills are issued immediately to the driver")
    verify(f.driver, times(5)).killTask(any)

    And("after receiving remaining terminal messages, all the tasks are killed eventually")
    tasks.foreach { task =>
      f.publishStatusUpdate(task, mesos.Protos.TaskState.TASK_KILLED)
    }

    verify(f.driver, times(5)).killTask(any)
    noMoreInteractions(f.driver)
  }

  test("kills will be retried") {
    val f = new Fixture
    val actor = f.createTaskKillActor()

    Given("a single, known running task")
    val task = f.mockTask(Task.Id.forRunSpec(f.appId), f.now(), mesos.Protos.TaskState.TASK_RUNNING)

    When("the service is asked to kill that task")
    actor ! TaskKillServiceActor.KillTask(task)

    Then("a kill is issued to the driver")
    verify(f.driver, timeout(500)).killTask(task.taskId.mesosTaskId)

    When("no statusUpdate is received and we reach the future")
    f.clock.+=(10.seconds)

    Then("the service will eventually retry")
    verify(f.driver, timeout(1000)).killTask(task.taskId.mesosTaskId)

    When("no statusUpdate is received and we reach the future")
    f.clock.+=(10.seconds)

    Then("the service will eventually expunge the task if it reached the max attempts")
    verify(f.stateOpProcessor, timeout(1000)).process(TaskStateOp.ForceExpunge(task.taskId))
  }

  override protected def afterAll(): Unit = {
    shutdown()
  }

  override protected def afterEach(): Unit = {
    import TaskKillServiceActorTest._
    actor match {
      case Some(actorRef) => system.stop(actorRef)
      case _ =>
        val msg = "The test didn't set a reference to the tested actor. Either make sure to set the ref" +
          "so it can be stopped automatically, or move the test to a suite that doesn't test this actor."
        fail(msg)
    }
  }

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(300, Millis)))

  class Fixture {
    val appId = PathId("/test")
    val taskTracker: TaskTracker = mock[TaskTracker]
    val driver = mock[SchedulerDriver]
    val driverHolder: MarathonSchedulerDriverHolder = {
      val holder = new MarathonSchedulerDriverHolder
      holder.driver = Some(driver)
      holder
    }
    val config: TaskKillConfig = new TaskKillConfig {
      import scala.concurrent.duration._
      override def killChunkSize: Int = 5
      override def killRetryTimeout: FiniteDuration = 500.millis
      override def killRetryMax: Int = 1
    }
    val stateOpProcessor: TaskStateOpProcessor = mock[TaskStateOpProcessor]
    val parent = TestProbe()
    val clock = ConstantClock()

    def createTaskKillActor(): ActorRef = {
      import TaskKillServiceActorTest._
      val actorRef: ActorRef = TestActorRef(TaskKillServiceActor.props(taskTracker, driverHolder, stateOpProcessor, config, clock), parent.ref, "TaskKillService")
      actor = Some(actorRef)
      actorRef
    }

    def mockTask(taskId: Task.Id, stagedAt: Timestamp, mesosState: mesos.Protos.TaskState): Task.LaunchedEphemeral = {
      val status: Task.Status = mock[Task.Status]
      status.stagedAt returns stagedAt
      val mesosStatus: mesos.Protos.TaskStatus = mesos.Protos.TaskStatus.newBuilder()
        .setState(mesosState)
        .buildPartial()
      val task = mock[Task.LaunchedEphemeral]
      task.taskId returns taskId
      task.status returns status
      task.mesosStatus returns Some(mesosStatus)
      task
    }
    def now(): Timestamp = Timestamp(0)
    def publishStatusUpdate(task: Task, state: mesos.Protos.TaskState): Unit = {
      val appId = task.taskId.runSpecId
      val statusUpdateEvent =
        MesosStatusUpdateEvent(
          slaveId = "", taskId = task.taskId, taskStatus = state.toString, message = "", appId = appId, host = "",
          ipAddresses = None, ports = Nil, version = "version"
        )
      system.eventStream.publish(statusUpdateEvent)
    }
  }
}

object TaskKillServiceActorTest {
  var actor: Option[ActorRef] = None
}