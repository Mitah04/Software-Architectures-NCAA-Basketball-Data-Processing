import java.nio.file.Paths
import java.time.{LocalDate, Year}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, Merge, Sink, Source}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object BasketballAnalysisApp extends App:
  implicit val actorSystem: ActorSystem = ActorSystem("BasketballAnalysis")
  implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

  // File path for input CSV file
  val csvFilePath: String = "src/main/resources/basketball.csv"

  // Function to check if the point difference is less than or equal to 5
  def isCloseGame(winPoints: Int, losePoints: Int): Boolean = Math.abs(winPoints - losePoints) <= 5

  // Source to read the CSV file as a stream of ByteStrings
  val csvSource: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get(csvFilePath))

  // Flow to parse CSV lines into a list of ByteStrings
  val csvLineParser: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()

  // Flow to map CSV lines to a key-value map based on headers
  val csvToMap: Flow[List[ByteString], Map[String, ByteString], NotUsed] = CsvToMap.toMap()

  // Flow to convert maps to GameRecord objects
  val gameRecordParser: Flow[Map[String, ByteString], GameRecord, NotUsed] = Flow[Map[String, ByteString]]
    .map(tempMap => {
      tempMap.map(element => {
        (element._1, element._2.utf8String)
      })
    }).map(record => {
      GameRecord(
        season = record("season").toInt,
        round = record("round").toInt,
        daysFromEpoch = record("days_from_epoch").toInt,
        gameDate = LocalDate.parse(record("game_date")),
        day = record("day"),
        winSeed = record("win_seed").toInt,
        winRegion = record("win_region").head,
        winMarket = record("win_market"),
        winName = record("win_name"),
        winAlias = record("win_alias"),
        winTeamId = record("win_team_id"),
        winSchoolNcaa = record("win_school_ncaa"),
        winCodeNcaa = record("win_code_ncaa").toInt,
        winKaggleTeamId = record("win_kaggle_team_id").toInt,
        winPoints = record("win_pts").toInt,
        loseSeed = record("lose_seed").toInt,
        loseRegion = record("lose_region").head,
        loseMarket = record("lose_market"),
        loseName = record("lose_name"),
        loseAlias = record("lose_alias"),
        loseTeamId = record("lose_team_id"),
        loseSchoolNcaa = record("lose_school_ncaa"),
        loseCodeNcaa = record("lose_code_ncaa").toInt,
        loseKaggleTeamId = record("lose_kaggle_team_id").toInt,
        losePoints = record("lose_pts").toInt,
        numOt = record("num_ot").toInt,
        academicYear = Year.parse(record("academic_year"))
      )
    })

  // Combine CSV source and parsing flows into a single source
  val gameRecordSource: Source[GameRecord, Future[IOResult]] = csvSource
    .via(csvLineParser)
    .via(csvToMap)
    .via(gameRecordParser)
    .buffer(20, OverflowStrategy.backpressure)

  // Debug flow to print each GameRecord
  val debugGameRecord: Flow[GameRecord, GameRecord, NotUsed] = Flow[GameRecord].map { record =>
    println(record)
    record
  }

  // Filters to process specific game criteria
  val sundayGamesFilter: Flow[GameRecord, GameRecord, NotUsed] = Flow[GameRecord].filter(_.day == "Sunday")

  val closeGamesFilter: Flow[GameRecord, GameRecord, NotUsed] = Flow[GameRecord].filter(game => isCloseGame(game.winPoints, game.losePoints))

  val quarterFinalsFilter: Flow[GameRecord, GameRecord, NotUsed] = Flow[GameRecord].filter(game => game.round <= 4)

  val seasonFilter: Flow[GameRecord, GameRecord, NotUsed] = Flow[GameRecord].filter(game => game.season >= 1980 && game.season <= 1990)

  // Flows to count victories and losses
  val countVictoriesFlow: Flow[GameRecord, Map[String, Int], NotUsed] =
    Flow[GameRecord].fold(Map.empty[String, Int]) { (acc, record) =>
      val teamId = record.winName + " " + record.winSchoolNcaa
      acc + (teamId -> (acc.getOrElse(teamId, 0) + 1))
    }

  val countLossesFlow: Flow[GameRecord, Map[String, Int], NotUsed] =
    Flow[GameRecord].fold(Map.empty[String, Int]) { (acc, record) =>
      val teamId = record.loseName + " " + record.loseSchoolNcaa
      acc + (teamId -> (acc.getOrElse(teamId, 0) + 1))
    }

  val countTeamsFlow: Flow[GameRecord, Map[String, Int], NotUsed] =
    Flow[GameRecord].fold(Map.empty[String, Int]) { (acc, record) =>
      val winTeamId = record.winName + " " + record.winSchoolNcaa
      val loseTeamId = record.loseName + " " + record.loseSchoolNcaa

      // Update both winning and losing teams' counts in the accumulator map
      acc
        .updated(winTeamId, acc.getOrElse(winTeamId, 0) + 1)
        .updated(loseTeamId, acc.getOrElse(loseTeamId, 0) + 1)
    }


  // Case class to represent output data
  case class GameStatistics(file: String, format: String, stats: Map[String, Int])

  val flowConvertDataSundays: Flow[Map[String, Int], GameStatistics, NotUsed] = Flow[Map[String, Int]].map { map =>
    GameStatistics(
      file = "src/main/resources/sundays.txt",
      format = "Name: %s -> Won Games on Sunday: %d\n",
      stats = map
    )
  }
  val flowConvertDataCloseGames: Flow[Map[String, Int], GameStatistics, NotUsed] = Flow[Map[String, Int]].map { map =>
    GameStatistics(
      file = "src/main/resources/close_games.txt",
      format = "Name: %s -> Games Won: %d\n",
      stats = map
    )
  }

  val flowConvertDataQuarterFinals: Flow[Map[String, Int], GameStatistics, NotUsed] = Flow[Map[String, Int]].map { map =>
    GameStatistics(
      file = "src/main/resources/quarter_finals.txt",
      format = "Name: %s -> Times Reached at least Quarter-Finals: %d\n",
      stats = map
    )
  }

  val flowConvertDataYears: Flow[Map[String, Int], GameStatistics, NotUsed] = Flow[Map[String, Int]].map { map =>
    GameStatistics(
      file = "src/main/resources/years.txt",
      format = "Name: %s -> Games Lost: %d\n",
      stats = map
    )
  }


  // Define parallel processing of game records
  val parallelProcessingFlow: Flow[GameRecord, GameStatistics, NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[GameRecord](4))
      val merge = builder.add(Merge[GameStatistics](4))

      broadcast.out(0) ~> sundayGamesFilter.async ~> countVictoriesFlow.async ~> flowConvertDataSundays ~> merge.in(0)
      broadcast.out(1) ~> closeGamesFilter.async ~> countVictoriesFlow.async ~> flowConvertDataCloseGames ~> merge.in(1)
      broadcast.out(2) ~> quarterFinalsFilter.async ~> countTeamsFlow.async ~> flowConvertDataQuarterFinals ~> merge.in(2)
      broadcast.out(3) ~> seasonFilter.async ~> countLossesFlow.async ~> flowConvertDataYears ~> merge.in(3)

      FlowShape(broadcast.in, merge.out)
    }
  )

  // Convert GameStatistics to ByteString for file writing
  def statisticsToByteString(stats: GameStatistics): ByteString = {
    val content = stats.stats.map { case (team, count) =>
      stats.format.format(team, count)
    }.mkString
    ByteString(content)
  }

  // Flow to write statistics to files
  val fileWriterFlow: Flow[GameStatistics, IOResult, NotUsed] = Flow[GameStatistics].mapAsync(1) { stats =>
    val bytes = statisticsToByteString(stats)
    println(s"Writing to file: ${stats.file}")
    Source.single(bytes).runWith(FileIO.toPath(Paths.get(stats.file)))
  }

  // Sink to handle writing results to files
  val fileSink: Sink[GameStatistics, Future[IOResult]] = Flow[GameStatistics]
      .via(fileWriterFlow)
      .fold(IOResult(0))((acc, next) => next)
      .toMat(Sink.head)(Keep.right)

  // Construct and run the stream graph
  val streamGraph = gameRecordSource
    .via(parallelProcessingFlow)
    .toMat(fileSink)(Keep.right)

  val result: Future[IOResult] = streamGraph.run()

  // Handle completion of the stream
  result.onComplete {
    case Success(IOResult(count, status)) =>
      println(s"Successfully wrote $count bytes")
      actorSystem.terminate()
    case Failure(exception) =>
      println(s"Stream processing failed: ${exception.getMessage}")
      actorSystem.terminate()
  }
