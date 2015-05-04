package com.fiser.akka.cluster

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.{Member, Cluster}
import akka.cluster.ClusterEvent.MemberUp
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try

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

  def register(member: Member): Unit =
    if (member.hasRole("worker"))
      self forward WorkerRegistration
}

object WorkProducer extends App {

  lazy val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  // Override the configuration of the port when specified as program argument
  val config = ConfigFactory.parseString("akka.cluster.roles = [producer]").
    withFallback(ConfigFactory.load())

  val system = ActorSystem("ClusterSystem", config)

  // Read cluster seed nodes from the file specified in the configuration
  val seeds = Try(config.getString("app.cluster.seedsFile")).toOption match {
    case Some(seedsFile) =>
      // Seed file was specified, read it
      log.info(s"reading seed nodes from file: $seedsFile")
      Source.fromFile(seedsFile).getLines().map { address =>
        AddressFromURIString.parse(s"akka.tcp://ClusterSystem@$address")
      }.toList
    case None =>
      // No seed file specified, use this node as the first seed
      log.info("no seed file found, using default seeds")
      val port = config.getInt("app.port")
      val localAddress = Try(config.getString("app.host"))
        .toOption.getOrElse(InetAddress.getLocalHost.getHostAddress)
      List(AddressFromURIString.parse(s"akka.tcp://ClusterSystem@$localAddress:$port"))
  }


  val producer = system.actorOf(Props[WorkProducer], name = "producer")

  val counter = new AtomicInteger

  import system.dispatcher

  system.scheduler.schedule(2 seconds, 2 seconds) {
    implicit val timeout = Timeout(5 seconds)
    (producer ? Job("hello-" + counter.incrementAndGet())) onSuccess {
      case result => println(result)
    }
  }
  log.info(s"Joining cluster with seed nodes: $seeds")
  Cluster.get(system).joinSeedNodes(seeds.toSeq)
  system.awaitTermination()
}
