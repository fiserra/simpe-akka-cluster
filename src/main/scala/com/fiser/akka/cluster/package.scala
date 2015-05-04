package com.fiser.akka

import java.net.InetAddress

import akka.actor.{Address, AddressFromURIString}
import com.typesafe.config.Config

import scala.io.Source
import scala.util.Try

package object cluster {

  val ClusterName = "ClusterSystem"

  object Roles extends Enumeration {
    type WeekDay = Value
    val worker, producer = Value
  }

  /**
   * Read cluster seed nodes from the file specified in the configuration
   */
  def getSeedsNodes(config: Config): List[Address] = {

    val seeds = Try(config.getString("app.cluster.seedsFile")).toOption match {
      case Some(seedsFile) =>
        // Seed file was specified, read it
        Source.fromFile(seedsFile).getLines().map { address =>
          AddressFromURIString.parse(s"akka.tcp://$ClusterName@$address")
        }.toList
      case None =>
        // No seed file specified, use this node as the first seed
        val port = config.getInt("app.port")
        val localAddress = Try(config.getString("app.host"))
          .toOption.getOrElse(InetAddress.getLocalHost.getHostAddress)
        List(AddressFromURIString.parse(s"akka.tcp://$ClusterName@$localAddress:$port"))
    }
    seeds
  }
}
