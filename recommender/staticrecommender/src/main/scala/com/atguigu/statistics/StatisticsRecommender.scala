package com.atguigu.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

case class MongoConfig(val uri:String, val db:String)

case class Recommendation(mid:Int, score:Double)
case class GenresRecommendation(genres:String, recs:Seq[Recommendation])

object StatisticsRecommender {

	val MONGODB_RATING_COLLECTION = "Rating"
	val MONGODB_MOVIE_COLLECTION = "Movie"

	//统计的表的名称
	val RATE_MORE_MOVIES = "RateMoreMovies"
	val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
	val AVERAGE_MOVIES = "AverageMovies"
	val GENRES_TOP_MOVIES = "GenresTopMovies"

	// 入口方法
	def main(args: Array[String]): Unit = {

		val config = Map(
			"spark.cores" -> "local[*]",
			"mongo.uri" -> "mongodb://hadoop101:27017/recommender",
			"mongo.db" -> "recommender"
		)

		//创建SparkConf配置
		val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
		//创建SparkSession
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		//加入隐式转换
		import spark.implicits._

		//数据加载进来
		val ratingDF = spark
			.read
			.option("uri", mongoConfig.uri)
			.option("collection", MONGODB_RATING_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load()
			.as[Rating]
			.toDF()

		val movieDF = spark
			.read
			.option("uri", mongoConfig.uri)
			.option("collection", MONGODB_MOVIE_COLLECTION)
			.format("com.mongodb.spark.sql")
			.load()
			.as[Movie]
			.toDF()

		//创建一张名叫ratings的表
		ratingDF.createOrReplaceTempView("ratings")

		//TODO: 不同的统计推荐结果

		spark.stop()
	}
}
