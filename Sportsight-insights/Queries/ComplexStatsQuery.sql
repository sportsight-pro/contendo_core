WITH
  numeratorStats AS (
  SELECT
    SportCode,
    StatSource,
    StatFunction,
    StatObject,
    StatTimeframe,
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    TeamCode,
    PlayerCode,
    StatName,
    SUM(StatValue) as StatValue
 FROM
 `Sportsight_Stats.all_stats`
  WHERE
    StatName in ({NumeratorStatNames})
    AND StatTimeframe in ({StatTimeframes})
    AND StatObject in ({StatObjects})
  GROUP BY
    SportCode,
    StatSource,
    StatFunction,
    StatObject,
    StatTimeframe,
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    TeamCode,
    PlayerCode,
    StatName
  ),
  denominatorStats AS (
  SELECT
    SportCode,
    StatSource,
    StatFunction,
    StatObject,
    StatTimeframe,
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    TeamCode,
    PlayerCode,
    StatName,
    SUM(StatValue) as StatValue
  FROM
    `Sportsight_Stats.all_stats`
  WHERE
    StatName in ({DenominatorStatNames})
    AND StatTimeframe in ({StatTimeframes})
    AND StatObject in ({StatObjects})
  GROUP BY
    SportCode,
    StatSource,
    StatFunction,
    StatObject,
    StatTimeframe,
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    TeamCode,
    PlayerCode,
    StatName
  ),
  numeratorDenominatorJoin AS (
  SELECT
    (
    SELECT
      AS STRUCT denominatorStats.*) AS denominatorStats,
    (
    SELECT
      AS STRUCT numeratorStats.*) AS numeratorStats
  FROM
    denominatorStats
  LEFT JOIN
    numeratorStats
  ON
    denominatorStats.SportCode = numeratorStats.SportCode
    #AND denominatorStats.StatSource = numeratorStats.StatSource
    #AND denominatorStats.StatFunction = numeratorStats.StatFunction
    AND denominatorStats.StatObject = numeratorStats.StatObject
    AND denominatorStats.StatTimeframe = numeratorStats.StatTimeframe
    AND denominatorStats.LeagueCode = numeratorStats.LeagueCode
    AND denominatorStats.SeasonCode = numeratorStats.SeasonCode
    AND denominatorStats.CompetitionStageCode = numeratorStats.CompetitionStageCode
    AND denominatorStats.CompetitionDay = numeratorStats.CompetitionDay
    AND denominatorStats.GameCode = numeratorStats.GameCode
    AND denominatorStats.GamePeriodCode = numeratorStats.GamePeriodCode
    AND denominatorStats.TeamCode = numeratorStats.TeamCode
    AND denominatorStats.PlayerCode = numeratorStats.PlayerCode
  WHERE
    numeratorStats.StatValue >= {MinimalNumeratorValue}
    AND denominatorStats.StatValue >= {MinimalDenominatorValue}
  ),
  newStatCalc AS (
  SELECT
    numeratorStats.SportCode,
    'ComposedStats' as StatSource,
    numeratorStats.StatFunction,
    numeratorStats.StatObject,
    numeratorStats.StatTimeframe,
    numeratorStats.LeagueCode,
    numeratorStats.SeasonCode,
    numeratorStats.CompetitionStageCode,
    numeratorStats.CompetitionDay,
    numeratorStats.GameCode,
    numeratorStats.GamePeriodCode,
    numeratorStats.TeamCode,
    numeratorStats.PlayerCode,
    '{StatName}' AS StatName,
    ROUND(AVG(IF(numeratorStats.StatValue IS NULL, 0, numeratorStats.StatValue)/denominatorStats.StatValue)*{StatRatio}, 2) AS StatValue,
    CAST(ROUND(SUM(denominatorStats.StatValue),0) AS int64) AS Count,
    #count(*) as Count,
    "{Description}" as Description
  FROM
    numeratorDenominatorJoin
   GROUP BY
    SportCode,
    StatSource,
    StatFunction,
    StatObject,
    StatTimeframe,
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    TeamCode,
    PlayerCode,
    StatName,
    Description
)
SELECT
    SportCode,
    StatSource,
    StatFunction,
    StatObject,
    StatTimeframe,
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    TeamCode,
    PlayerCode,
    StatName,
    StatValue,
    Count,
    Description,
    DENSE_RANK() OVER (PARTITION BY StatObject, LeagueCode, SeasonCode, CompetitionStageCode, StatName ORDER BY StatValue DESC) AS DenseRank
FROM
    newStatCalc
ORDER BY
  DenseRank ASC
