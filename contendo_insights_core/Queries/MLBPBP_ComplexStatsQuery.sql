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
    BaseStat,
    ConditionCode,
    ObjectType,
    SUM(StatValue) as StatValue
 FROM
 `Sportsight_Stats.all_stats_baseball`
  WHERE
    BaseStat in ({NumeratorStatNames})
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
    StatName,
    BaseStat,
    ConditionCode,
    ObjectType
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
    BaseStat,
    ConditionCode,
    ObjectType,
    SUM(StatValue) as StatValue
  FROM
    `Sportsight_Stats.all_stats_baseball`
  WHERE
    BaseStat in ({DenominatorStatNames})
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
    StatName,
    BaseStat,
    ConditionCode,
    ObjectType
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
    AND denominatorStats.ConditionCode = numeratorStats.ConditionCode
    AND denominatorStats.ObjectType = numeratorStats.ObjectType
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
    FORMAT('pbp.%s.%s.%s', '{StatName}', numeratorStats.ObjectType, numeratorStats.ConditionCode) AS StatName,
    ROUND(AVG(IF(numeratorStats.StatValue IS NULL, 0, numeratorStats.StatValue)/denominatorStats.StatValue)*{StatRatio}, 2) AS StatValue,
    CAST(ROUND(SUM(numeratorStats.StatValue),0) AS int64) AS Count,
    #count(*) as Count,
    "{Description}" as Description,
    '{StatName}' AS BaseStat,
    numeratorStats.ConditionCode,
    numeratorStats.ObjectType
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
    Description,
    BaseStat,
    ConditionCode,
    ObjectType
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
    BaseStat,
    ConditionCode,
    ObjectType,
    DENSE_RANK() OVER (PARTITION BY StatObject, LeagueCode, SeasonCode, CompetitionStageCode, StatName ORDER BY StatValue DESC) AS DenseRank
FROM
    newStatCalc
ORDER BY
  DenseRank ASC
