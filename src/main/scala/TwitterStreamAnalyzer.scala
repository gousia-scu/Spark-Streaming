import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkConf, SparkContext}


object TwitterStreamAnalyzer {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("twitter-stream-sentiment").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")



    val spark = SparkSession.builder.appName("twitter-sentiment-analysis").getOrCreate()
    val ssc = new StreamingContext(sc, Seconds(5))
    if (args.length < 4) {
      System.err.println("Usage: Demo <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val twitterStream = TwitterUtils.createStream(ssc,None)
    val englishTweets = twitterStream.filter(_.getLang == "en")
    englishTweets.map(_.getText).print()
    englishTweets.count().print()
//USING THE SLIDING WINDOW
//    val hashTags	=	englishTweets.flatMap(status	=>	status.getHashtagEntities.map(_.getText.toLowerCase))
//    val tagCounts	=	hashTags.window(Minutes(1),	Seconds(5)).countByValue()

    val dataDS = englishTweets.map { tweet =>
      val sentiment = NLPManager.detectSentiment(tweet.getText)
      val tags = tweet.getHashtagEntities.map(_.getText.toLowerCase)
      println("(" + tweet.getText + " | " + sentiment.toString + " | " + tags.toString())
      (tweet.getText, sentiment.toString, tags)
    }


    val sqlContex = spark.sqlContext

//    var dataRDD : org.apache.spark.rdd.RDD[(String,String,Array[String])] = sc.emptyRDD
    dataDS.cache().foreachRDD(rdd => {
      val df = spark.createDataFrame(rdd)
//      df.show()
      df.createOrReplaceTempView("sentiments")

      sqlContex.sql("select * from sentiments limit 20").show()
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
