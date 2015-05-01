package com.fiser.akka.cluster

case class Job(text: String)
case class Result(text: String)
case class JobFailed(reason: String, job: Job)
case object WorkerRegistration