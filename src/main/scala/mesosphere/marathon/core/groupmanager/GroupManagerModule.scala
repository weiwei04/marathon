package mesosphere.marathon.core.groupmanager

import akka.actor.ActorRef
import akka.event.EventStream
import com.codahale.metrics.Gauge
import mesosphere.marathon.core.groupmanager.impl.{ GroupManagerActor, GroupManagerDelegate }
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.{ MarathonConf, MarathonSchedulerService }
import mesosphere.marathon.io.storage.StorageProvider
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.{ AppRepository, GroupRepository }
import mesosphere.util.CapConcurrentExecutions

import scala.concurrent.Await

/**
  * Provides a [[GroupManager]] implementation.
  */
class GroupManagerModule(
    config: MarathonConf,
    leadershipModule: LeadershipModule,
    serializeUpdates: CapConcurrentExecutions,
    scheduler: MarathonSchedulerService,
    groupRepo: GroupRepository,
    appRepo: AppRepository,
    storage: StorageProvider,
    eventBus: EventStream,
    metrics: Metrics) {

  private[this] val groupManagerActorRef: ActorRef = {
    val props = GroupManagerActor.props(
      serializeUpdates,
      scheduler,
      groupRepo,
      appRepo,
      storage,
      config,
      eventBus)
    leadershipModule.startWhenLeader(props, "groupManager")
  }

  val groupManager: GroupManager = {
    val groupManager = new GroupManagerDelegate(config, groupManagerActorRef)

    metrics.gauge("service.mesosphere.marathon.app.count", new Gauge[Int] {
      override def getValue: Int = {
        Await.result(groupManager.rootGroup(), config.zkTimeoutDuration).transitiveApps.size
      }
    })

    metrics.gauge("service.mesosphere.marathon.group.count", new Gauge[Int] {
      override def getValue: Int = {
        Await.result(groupManager.rootGroup(), config.zkTimeoutDuration).transitiveGroups.size
      }
    })

    metrics.gauge("service.mesosphere.marathon.uptime", new Gauge[Long] {
      val startedAt = System.currentTimeMillis()

      override def getValue: Long = {
        System.currentTimeMillis() - startedAt
      }
    })

    groupManager
  }
}