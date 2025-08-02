import java.time.{LocalDate, Year}

case class GameRecord(
                       season : Int,
                       round : Int,
                       daysFromEpoch : Int,
                       gameDate : LocalDate,
                       day : String,
                       winSeed : Int,
                       winRegion : Char,
                       winMarket : String,
                       winName : String,
                       winAlias : String,
                       winTeamId : String,
                       winSchoolNcaa : String,
                       winCodeNcaa : Int,
                       winKaggleTeamId : Int,
                       winPoints : Int,
                       loseSeed : Int,
                       loseRegion : Char,
                       loseMarket : String,
                       loseName : String,
                       loseAlias : String,
                       loseTeamId : String,
                       loseSchoolNcaa : String,
                       loseCodeNcaa : Int,
                       loseKaggleTeamId : Int,
                       losePoints : Int,
                       numOt : Int,
                       academicYear : Year
)

