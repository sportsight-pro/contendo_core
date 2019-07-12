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
    MatchCode,
    MatchStageCode,
    TeamCode,
    PlayerCode,
    StatName,
    SUM(StatValue) as StatValue,
    Count,
    Description
 FROM
 `Sportsight_Stats.all_stats`
  WHERE
    STRPOS('{NumeratorStatNames}', StatName) > 0
    AND STRPOS('{StatTimeframe}', StatTimeframe) > 0
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
    MatchCode,
    MatchStageCode,
    TeamCode,
    PlayerCode,
    StatName,
    Count,
    Description
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
    MatchCode,
    MatchStageCode,
    TeamCode,
    PlayerCode,
    StatName,
    SUM(StatValue) as StatValue,
    Count,
    Description
  FROM
    `Sportsight_Stats.all_stats`
  WHERE
    STRPOS('{DenominatorStatNames}', StatName) > 0
    AND STRPOS('{StatTimeframe}', StatTimeframe) > 0
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
    MatchCode,
    MatchStageCode,
    TeamCode,
    PlayerCode,
    StatName,
    Count,
    Description
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
    AND denominatorStats.StatSource = numeratorStats.StatSource
    AND denominatorStats.StatObject = numeratorStats.StatObject
    AND denominatorStats.StatTimeframe = numeratorStats.StatTimeframe
    AND denominatorStats.LeagueCode = numeratorStats.LeagueCode
    AND denominatorStats.SeasonCode = numeratorStats.SeasonCode
    AND denominatorStats.CompetitionStageCode = numeratorStats.CompetitionStageCode
    AND denominatorStats.CompetitionDay = numeratorStats.CompetitionDay
    AND denominatorStats.MatchStageCode = numeratorStats.MatchStageCode
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
    numeratorStats.MatchCode,
    numeratorStats.MatchStageCode,
    numeratorStats.TeamCode,
    numeratorStats.PlayerCode,
    '{StatName}' AS StatName,
    ROUND((IF(numeratorStats.StatValue IS NULL, 0, numeratorStats.StatValue)/denominatorStats.StatValue)*{StatRatio}, 2) AS StatValue,
    CAST(ROUND(SUM(denominatorStats.StatValue),0) AS int64) AS Count,
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
    MatchCode,
    MatchStageCode,
    TeamCode,
    PlayerCode,
    StatName,
    Count,
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
    MatchCode,
    MatchStageCode,
    TeamCode,
    PlayerCode,
    StatName,
    StatValue,
    Count,
    Description,
    DENSE_RANK() OVER (PARTITION BY statlevel, LeagueCode, seasonid, CompetitionPhase, MetricName ORDER BY StatValue DESC) AS DenseRank
FROM
    newStatCalc
ORDER BY
  DenseRank ASC
