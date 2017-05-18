package me.scottlove.spark


import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object MusicRecommender {



  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    //Confiure spark session with needed memory and other
    val spark = SparkSession
      .builder()
      .appName("me.scottlove.spark.MusicRecommender")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.driver.memory", "4g")

    //Import Data Sets
    val rawArtistUserData = spark.read.textFile("data/user_artist_data.txt").repartition(8)
    val rawArtistData = spark.read.textFile("data/artist_data.txt").repartition(8)
    val rawArtistAlias = spark.read.textFile("data/artist_alias.txt")

    val musicRecommender = new MusicRecommender(spark)
    musicRecommender.model(rawArtistUserData, rawArtistAlias)
  }
}

class MusicRecommender(private val spark: SparkSession){
  import spark.implicits._

  def buildCounts(rawUserArtistData: Dataset[String], bArtistAlias: Broadcast[scala.collection.immutable.Map[Int,Int]]): DataFrame = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      (userID, finalArtistID, count)
    }.toDF("user", "artist", "count")
  }

  def preparation(rawArtistUserData: Dataset[String], rawArtistData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {

    rawArtistUserData.take(5).foreach(println)

    var artistAlias = rawArtistAlias.flatMap { line =>
      val Array(artist, alias) = line.split('\t')
      if(artist.isEmpty){
        None
      } else {
        Some((artist.toInt, alias.toInt))
      }
    }.collect().toMap

    val artistById = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if(name.isEmpty){
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")

    val userArtistDF = rawArtistUserData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")

  }

  def model(rawArtistUserData: Dataset[String], rawArtistAlias: Dataset[String]): Unit = {
    var artistAlias = rawArtistAlias.flatMap { line =>
      val Array(artist, alias) = line.split('\t')
      if(artist.isEmpty){
        None
      } else {
        Some((artist.toInt, alias.toInt))
      }
    }.collect().toMap
    val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
    val trainData = buildCounts(rawArtistUserData, bArtistAlias).cache()
    trainData.show()
  }
}
