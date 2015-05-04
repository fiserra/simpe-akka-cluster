package com.fiser.akka.cluster

import java.net.InetAddress

import akka.actor._
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.{Cluster, Member}
import com.typesafe.config.ConfigFactory

import scala.io.Source
import scala.util.Try

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

object Worker extends App {

  lazy val log = org.slf4j.LoggerFactory.getLogger(this.getClass.getName)


  // Override the configuration of the port when specified as program argument

  val config = ConfigFactory.parseString("akka.cluster.roles = [worker]").
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

  system.actorOf(Props[Worker], name = "worker")
  log.info(s"Joining cluster with seed nodes: $seeds")
  Cluster.get(system).joinSeedNodes(seeds.toSeq)


  system.awaitTermination()
}
