import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketTextStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Socket Text Stream")
    val sparkStreamingContext = new StreamingContext(conf, Seconds(5))

    val lines = sparkStreamingContext.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordFreqMapInit = words.map(word => (word, 1))

    val wordsCount = wordFreqMapInit.reduceByKey(_ + _)

    wordsCount.print()

    // This processor logic will work in batch of 5 seconds(given above). This means, after every 5 seconds, we will get new result
//    processorLogic(sparkStreamingContext)

    // Start the context, till termination
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()


  }

  // Need to block port number 9999, using the command: nc -lk <port_no>
  private def processorLogic(sparkStreamingContext: StreamingContext): Unit = {
    val lines = sparkStreamingContext.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordFreqMapInit = words.map(word => (word, 1))

    val wordsCount = wordFreqMapInit.reduceByKey(_ + _)

    wordsCount.print()
  }
}
