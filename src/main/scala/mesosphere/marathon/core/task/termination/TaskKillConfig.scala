package mesosphere.marathon.core.task.termination

import org.rogach.scallop.ScallopConf
import scala.concurrent.duration._

import scala.concurrent.duration.FiniteDuration

trait TaskKillConfig extends ScallopConf {
  //scalastyle:off magic.number

  private[this] lazy val _killChunkSize = opt[Int](
    "kill_chunk_size",
    descr = "INTERNAL TUNING PARAMETER: " +
      "The maximum number of concurrently processed kills",
    noshort = true,
    hidden = true,
    default = Some(100)
  )

  private[this] lazy val _killRetryTimeout = opt[Long](
    "kill_retry_timeout",
    descr = "INTERNAL TUNING PARAMETER: " +
      "The timeout after which a task kill will be retried.",
    noshort = true,
    hidden = true,
    default = Some(10000L) // 10 seconds
  )

  private[this] lazy val _killRetryMax = opt[Int](
    "kill_retry_max",
    descr = "INTERNAL TUNING PARAMETER: " +
      "The maximum number of kill retries before which a task will be forcibly expunged from state.",
    noshort = true,
    hidden = true,
    default = Some(100)
  )

  def killChunkSize: Int = _killChunkSize()
  def killRetryTimeout: FiniteDuration = _killRetryTimeout().millis
  def killRetryMax: Int = _killRetryMax()
}
