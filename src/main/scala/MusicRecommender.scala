
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.broadcast._


object MusicRecommender {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .appName("MusicRecommender")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val rawArtistData = spark.read.textFile("data/user_artist_data.txt")

    rawArtistData.take(5).foreach(println)

  }
}