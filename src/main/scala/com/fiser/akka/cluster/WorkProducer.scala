package com.fiser.akka.cluster

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class WorkProducer extends Actor with ActorLogging {

  private var workers = IndexedSeq.empty[ActorRef]
  private var jobCounter = 0

  def receive = {
    case job: Job if workers.isEmpty =>
      sender() ! JobFailed("Service unavailable, try again later", job)

    case job: Job =>
      jobCounter += 1
      workers(jobCounter % workers.size) forward job

    case WorkerRegistration if !workers.contains(sender()) =>
      context watch sender()
      workers = workers :+ sender()

    case Terminated(a) =>
      workers = workers.filterNot(_ == a)
  }
}

object WorkProducer {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [producer]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val producer = system.actorOf(Props[WorkProducer], name = "producer")

    val counter = new AtomicInteger
    import system.dispatcher
    system.scheduler.schedule(2 seconds, 2 seconds) {
      implicit val timeout = Timeout(5 seconds)
      (producer ? Job("hello-" + counter.incrementAndGet())) onSuccess {
        case result => println(result)
      }
    }
  }
}
