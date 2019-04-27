package streaming

import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import streaming.TwitterStreaming.Sentiment.Sentiment

import java.util.HashMap
import java.util.Properties

import scala.collection.convert.wrapAll._

object TwitterStreaming {
  def main(args: Array[String]) {
    import Utils._

    if (args.length < 2) {
      System.out.println("Usage: TwitterSparkStreaming <Topic1> <Topic2>")
      return
    }

    val topic = args(0).toString
    val filters = args.slice(1, args.length)
    /*localhost:2181, localhost:2181*/
    val kafkaBrokers = "127.0.1.1:9092,127.0.1.1:9093"

    // Configure Spark
    val sparkConfiguration = new SparkConf().
      setAppName("spark-twitter-stream-example").
      setMaster(sys.env.get("spark.master").getOrElse("local[*]"))

    // Let's create the Spark Context using the configuration we just created
    val sparkContext = new SparkContext(sparkConfiguration)
    sparkContext.setLogLevel("ERROR")

    // Now let's wrap the context in a streaming one, passing along the window size
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    val tweets: DStream[Status] =
    TwitterUtils.createStream(streamingContext, None, filters)

    // Let's extract the words of each tweet
    val textAndSentences: DStream[(TweetText, Sentence)] =
    tweets.filter(x => x.getLang == "en").
      map(_.getText).
      map(tweetText => (tweetText, wordsOf(tweetText)))

    textAndSentences.print()

    //Sentiment Analysis on tweets:
      def mainSentiment(input: String): Sentiment = Option(input) match {
        case Some(text) if !text.isEmpty => extractSentiment(text)
        case _ => throw new IllegalArgumentException("input can't be null or empty")
      }

      def extractSentiment(text: String): Sentiment = {
        val (_, sentiment) = extractSentiments(text)
          .maxBy { case (sentence, _) => sentence.length }
        //println("Shayan returning sentiment" + sentiment)
        sentiment
      }


      def extractSentiments(text: String): List[(String, Sentiment)] = {
        val props = new Properties()
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
        val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
        //println("Shayan Initialized pipeline ")

        val annotation: Annotation = pipeline.process(text)
        //println("Shayan processed text")

        val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
        sentences.map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
          .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
          .toList
        //println("Shayan processed sentences")
      }


    // send data to Kafka broker
    textAndSentences.foreachRDD( rdd => {
      rdd.foreachPartition( partition => {
        // Print statements in this section are shown in the executor's stdout logs
        val props = new HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach( record => {
          val data = record.toString
          //print("Data before split "+ data)
          val realTweet = data.split(",")(0)
          //println("The data in tweet is " + data.split(",")(0))
          val senti = mainSentiment(realTweet)
          println("sending to kafka: Sentiment is: " + senti.toString)
          val message = new ProducerRecord[String, String](topic, null, senti.toString)
          producer.send(message)
        })
        producer.close()
      })

    })
    // Start streaming
    streamingContext.start()

    // Await termination
    streamingContext.awaitTermination()
  }

  object Sentiment extends Enumeration {
    type Sentiment = Value
    val POSITIVE, NEGATIVE, NEUTRAL = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
      case x if x == 0 || x == 1 => Sentiment.NEGATIVE
      case 2 => Sentiment.NEUTRAL
      case x if x == 3 || x == 4 => Sentiment.POSITIVE
    }
  }
}
