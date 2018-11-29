package com.test.spark.wiki.extracts

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings.createTempView("leagues")
    session.sql("select season, league, avg(goalsFor) from leagues group by season, league order by league, season").show

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")

    //standings.where($"league"==="Ligue 1").select("team").distinct().show(100)
    //standings.where($"league"==="Ligue 1" && $"team"==="Lyon").show()
    println(standings.filter($"league"==="Ligue 1").filter($"position"===1).groupBy("team").count.orderBy(desc("count")).first.toString)

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    standings.where($"position"===1).groupBy("league").avg("points").show
    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val dec:String=>String = _.replaceAll(".$", "X")
    val decUDF = udf(dec)
    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")
    val tbl = standings.withColumn("decennie", decUDF('season))
    val firstPoints: Dataset[Row] =tbl.where($"position"===1).groupBy("decennie","league").agg(avg("points").alias("avg1"))
    firstPoints.join(tbl.where($"position"===10).groupBy("decennie","league").agg(avg("points").alias("avg10"))
                    ,usingColumns = Seq("decennie","league"))
              .withColumn("diffPoints",$"avg1"-$"avg10")
              .orderBy("league","decennie").show(25)
  }
}
