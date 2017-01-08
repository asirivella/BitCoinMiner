import java.io.File
import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.routing.RoundRobinRouter
import java.security.MessageDigest

sealed trait BitCoin
case class WorkScheduler(serverAddress: String, master: ActorRef) extends BitCoin
case class ResultScheduler(serverAddress: String) extends BitCoin
case class Master(workerCount: Int, workUnits: Int, ResultScheduler: ActorRef) extends BitCoin
case class RequestWork() extends BitCoin
case class RemoteMessage(buffer: ArrayBuffer[String]) extends BitCoin
case class MiningResult(buffer: ArrayBuffer[String]) extends BitCoin
case class AllocateWork(leadingZeroCount: Int) extends BitCoin
case class InitiateMining(leadingZeroCount: Int) extends BitCoin
case class BitcoinMining(stringLength: Int, leadingZeroCount: Int) extends BitCoin
case class PublishMessage(buffer: ArrayBuffer[String]) extends BitCoin

object BitcoinMiningClient extends App{

			class WorkScheduler(serverAddress: String, master:ActorRef) extends Actor {

		val bitCoinServer = context.actorFor(serverAddress)

				def receive = {

				case RequestWork() =>
				bitCoinServer ! "ClientAlive"

				case AllocateWork(leadingZeroCount) =>
				println("---------------Received work from the Bitcoin server-------------")
				master ! InitiateMining(leadingZeroCount)

				case _ =>
				println("Invalid message recieved from the Server")
		}
	}

	class ResultScheduler(serverAddress: String) extends Actor{

		val bitCoinServer = context.actorFor(serverAddress)

				def receive = {

				case MiningResult(buffer) =>
				bitCoinServer ! RemoteMessage(buffer)
				context.system.shutdown()

				case _ =>
				println("Invalid message propagated through client")
		}
	}

	class Master(workerCount: Int, workUnits: Int, ResultScheduler: ActorRef) extends Actor{

		var buffer: ArrayBuffer[String] = new ArrayBuffer[String]()
				val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(workerCount)), name = "workerRouter")
				var resultCount: Int = 0
				def receive = {

				case InitiateMining(leadingZeroCount) =>
				for(i <- 1 to workUnits)
					workerRouter ! BitcoinMining(10 + i, leadingZeroCount)

				case PublishMessage(result) =>
				buffer  = buffer ++ result
				resultCount += 1
				if(resultCount == workUnits){
					println("--------------All work units exhausted------------")
					ResultScheduler ! MiningResult(buffer)
					context.stop(self)
				}
				}
	}

	class Worker extends Actor{

		def receive = {

		case BitcoinMining(stringLength, leadingZeroCount) =>
		sender ! PublishMessage(mineCoins(stringLength, leadingZeroCount))

		case _ =>
		println("Invalid Message propagated in Client System")
		}
	}

	def mineCoins(stringLength: Int, leadingZeroCount: Int) : ArrayBuffer[String] = {

			var output: ArrayBuffer[String] = new ArrayBuffer[String]()
					var targetPattern = generateTargetPattern(leadingZeroCount)

					for(i <- 0 until 100000){

						var key = generateRandomStringOfLength(stringLength)
								var hashString = hex_digest(key)
								if(isBitCoin(hashString,targetPattern))
									output += ("%s\t%s".format(key,hashString))        
					}
	output
	}

	def isBitCoin(hashString: String, targetPattern: String) : Boolean = {

			return (hashString.substring(0, targetPattern.length()) == targetPattern)
	}

	def generateTargetPattern(leadingZeroCount: Int): String = {

			var targetPattern = new StringBuilder()
			for(i <- 1 to leadingZeroCount){
				targetPattern.append("0")
			}
			return targetPattern.toString()
	}

	def hex_digest(s: String): String = {
    val sha = MessageDigest.getInstance("SHA-256")
			sha.digest(s.getBytes())
			.foldLeft("")((s: String, b: Byte) => s +
			Character.forDigit((b & 0xf0) >> 4, 16) +
			Character.forDigit(b & 0x0f, 16))
	}

	def generateRandomStringOfLength(length:Int): String ={
			val r = new scala.util.Random     
					val sb = new StringBuilder     
					sb.append("prajput+asirivella")
					for (i <- 1 to length) {
						sb.append(r.nextPrintableChar)
					}
			sb.toString()              
	}


	override def main(args: Array[String]) {

		if(!args.isEmpty && !args(0).isEmpty()){

			var workUnits = 25
					var workerCount = 2

					println("-----------String Bitcoin Mining Client ---------------------")
					println("-------- workerCount " + workerCount + "------ workUnits " + workUnits + "-----------")

					val configFile = getClass.getClassLoader.getResource("application.conf").getFile
					val config = ConfigFactory.parseFile(new File(configFile))
					val system = ActorSystem("BitcoinMiningClient",config)
					var serverAddress = "akka.tcp://BitCoinMiningServer@" + args(0) + ":5190/user/ClientWorkScheduler" 
          println(serverAddress)
					val ResultScheduler = system.actorOf(Props(new ResultScheduler(serverAddress)), name = "ResultScheduler")
					val master = system.actorOf(Props(new Master(workerCount, workUnits, ResultScheduler)),name = "master")
					val WorkScheduler = system.actorOf(Props(new WorkScheduler(serverAddress, master)), name = "WorkScheduler") 

					WorkScheduler ! RequestWork()

		}else{
			println("Server IP address needed to start client")
		}
	}
}