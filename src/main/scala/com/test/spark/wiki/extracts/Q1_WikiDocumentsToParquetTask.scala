package com.test.spark.wiki.extracts


import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.slf4j.{Logger, LoggerFactory}


case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

    import session.implicits._

    getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }
      .flatMap {
        case (season, (league, url)) =>
          try {
            // TODO Q2 Implémenter le parsing Jsoup. Il faut retourner une Seq[LeagueStanding]
            // ATTENTION:
            //  - Quelques pages ne respectent pas le format commun et pourraient planter - pas grave
            //  - Il faut veillez à recuperer uniquement le classement général
            //  - Il faut normaliser les colonnes "positions", "teams" et "points" en cas de problèmes de formatage

            val doc = Jsoup.connect(url).get()
            val tbl = doc.select("caption:contains(Classement)")
            var table = doc.select("table.wikitable").first()
            if (!tbl.isEmpty){
              table = tbl.first().parent()
            }
            val lines = table.select("td")
            val header = table.select("tr").first
            val hsize = header.select("th").size
            val htdsize = header.select("td").size
            var res: Seq[LeagueStanding] = Seq()
            val pattern = "([0-9]+)".r
            for( i <- (htdsize until lines.size by (hsize+htdsize))){
              val position = pattern.findFirstMatchIn(lines.get(i).text()).get
              val points = pattern.findFirstMatchIn(lines.get(i+2).text()).get
              res = res :+ LeagueStanding(
                  league = league,
                  season = season,
                  position = position.toString.toInt,
                  team = lines.get(i+1).select("a[title]").attr("title"),
                  points = points.toString.toInt,
                  played = lines.get(i+3).text().toInt,
                  won = lines.get(i+4).text().toInt,
                  drawn = lines.get(i+5).text().toInt,
                  lost = lines.get(i+6).text().toInt,
                  goalsFor = lines.get(i+7).text().toInt,
                  goalsAgainst = lines.get(i+8).text().toInt,
                  goalsDifference = lines.get(i+9).text().toInt
                )
            }
            res
          } catch {
            case _: Throwable =>
              // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
              // par défaut les logs sont stockés sur le noeud master dans /mnt/var/log/
              logger.warn(s"Can't parse season $season from $url")
              Seq.empty
          }
      }
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .coalesce(numPartitions = 2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
    // stockage et compression par colonne
    // chargement rapide des données
    // une exécution rapide des requêtes
    // une utilisation très efficace de l’espace de stockage
    // compatible avec toutes les interfaces MapReduce
    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    // un dataset par construction represente mieux les tables qu'une séquence.
    // les traitements de données en dataset sont inspirés des techniques d’évaluation de requêtes dans les systèmes relationnels.
  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val inputStream = getClass.getClassLoader.getResourceAsStream("leagues.yaml")
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name")  name: String, @JsonProperty("url")  url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
