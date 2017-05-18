
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j._


object MusicRecommender {

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    //confiure spark session with needed memory and other
    val spark = SparkSession
      .builder()
      .appName("MusicRecommender")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.driver.memory", "4g")
    import spark.implicits._

    val rawArtistData = spark.read.textFile("data/user_artist_data.txt").repartition(8)

    val userArtistDF = rawArtistData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")

    userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()


  }
}