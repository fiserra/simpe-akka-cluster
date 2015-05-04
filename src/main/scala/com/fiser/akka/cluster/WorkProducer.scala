package com.fiser.akka.cluster

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class WorkProducer extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  private var workers = IndexedSeq.empty[ActorRef]
  private var jobCounter = 0

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case job: Job if workers.isEmpty =>
      sender() ! JobFailed("Service unavailable, try again later", job)

    case job: Job =>
      jobCounter += 1
      workers(jobCounter % workers.size) forward job

    case WorkerRegistration if !workers.contains(sender()) =>
      context watch sender()
      workers = workers :+ sender()

    case MemberUp(m) =>
      register(m)

    case Terminated(a) =>
      workers = workers.filterNot(_ == a)
  }

  private def register(member: Member): Unit =
    if (member.hasRole(Roles.worker.toString))
      self forward WorkerRegistration
}

object WorkProducer extends App {

  private lazy val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  private val config = ConfigFactory.parseString(s"akka.cluster.roles = [${Roles.producer}]").
    withFallback(ConfigFactory.load())

  private val system = ActorSystem(ClusterName, config)

  private val producer = system.actorOf(Props[WorkProducer], name = "producer")

  private val counter = new AtomicInteger

  import system.dispatcher

  system.scheduler.schedule(1 seconds, 1 seconds) {
    implicit val timeout = Timeout(5 seconds)
    (producer ? Job("hello-" + counter.incrementAndGet())) onSuccess {
      case result => println(result)
    }
  }

  private val seedNodes = getSeedsNodes(config)

  log.info(s"Joining cluster with seed nodes: $seedNodes")
  Cluster.get(system).joinSeedNodes(seedNodes.toSeq)
  system.awaitTermination()
}
