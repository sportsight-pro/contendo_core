SELECT
  *,
  DENSE_RANK() OVER (PARTITION BY LeagueCode, SeasonCode, StatName ORDER BY StatValue DESC) AS DenseRank
FROM (
  SELECT
    '{SportCode}' AS SportCode,
    '{StatSource}' AS StatSource,
    '{StatFunction}' AS StatFunction,
    '{StatObject}' AS StatObject,
    '{StatTimeframe}' AS StatTimeframe,
    '{LeagueCode}' AS LeagueCode,
    Season AS SeasonCode,
    'N/A' as CompetitionStageCode,
    {DaysRange} as CompetitionDay,
    CAST({GameCode} as STRING) AS GameCode,
    'N/A' as GamePeriodCode,
    CAST(team.id AS STRING) AS TeamCode,
    CAST({PlayerCode} AS STRING) AS PlayerCode,
    '{StatName}' AS StatName,
    ROUND({StatFunction}(stats.{StatName}),2) AS StatValue,
    Countif(stats.{StatName}>0) as Count,
    '{Description}' as Description,
    'N/A' AS BaseStat,
    'N/A' AS ConditionCode,
    'N/A' AS ObjectType
  FROM
    `sportsight-tests.Baseball1.daily_{StatObject}_gamelogs_*`
  LEFT JOIN
    unnest ( gamelogs )
  WHERE
    {StatCondition}
  GROUP BY
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    teamCode,
    playerCode,
    StatName
    ) WHERE StatValue is not NULL and StatValue > 0
