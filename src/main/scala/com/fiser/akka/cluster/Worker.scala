package com.fiser.akka.cluster

import akka.actor._
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory

class Worker extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  // subscribe to cluster changes, MemberUp
  // re-subscribe when restart
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case job@Job(text: String) =>
      log.info("Received: {}", job)
      sender() ! Result(text.toUpperCase)
    case MemberUp(m) =>
      register(m)
  }

  def register(member: Member): Unit =
    if (member.hasRole("producer"))
      context.actorSelection(RootActorPath(member.address) / "user" / "producer") ! WorkerRegistration
}

object Worker {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    system.actorOf(Props[Worker], name = "worker")
  }
}
