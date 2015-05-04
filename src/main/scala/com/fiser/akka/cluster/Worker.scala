package com.fiser.akka.cluster

import akka.actor._
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory

class Worker extends Actor with ActorLogging {

  private val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case job@Job(text: String) =>
      log.info("Received: {}", job)
      sender() ! Result(text.toUpperCase)
    case MemberUp(m) =>
      register(m)
  }

  private def register(member: Member): Unit =
    if (member.hasRole(Roles.producer.toString))
      context.actorSelection(RootActorPath(member.address) / "user" / "producer") ! WorkerRegistration
}

object Worker extends App {

  private lazy val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)

  private val config = ConfigFactory.parseString(s"akka.cluster.roles = [${Roles.worker}]").
    withFallback(ConfigFactory.load())

  private val system = ActorSystem(ClusterName, config)

  system.actorOf(Props[Worker], name = "worker")

  private val seedNodes = getSeedsNodes(config)
  log.info(s"Joining cluster with seed nodes: $seedNodes")
  Cluster.get(system).joinSeedNodes(seedNodes.toSeq)
  system.awaitTermination()
}
