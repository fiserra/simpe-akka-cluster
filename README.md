
> runMain com.fiser.akka.cluster.WorkProducer
> runMain com.fiser.akka.cluster.Worker 2551
> runMain com.fiser.akka.cluster.Worker 2552


java -cp simple-akka-cluster-assembly-1.0.jar com.fiser.akka.cluster.Worker -DAPP_PORT=8081 -DAPP_SEEDS_FILE=seeds.txt
java -cp simple-akka-cluster-assembly-1.0.jar com.fiser.akka.cluster.WorkProducer -DAPP_PORT=8080 -DAPP_SEEDS_FILE=seeds.txt
