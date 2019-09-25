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
    CAST({TeamType}.id AS STRING) AS TeamCode,
    CAST({PlayerProperty} AS STRING) AS PlayerCode,
    '{StatName}' AS StatName,
    ROUND({StatFunction}({PropertyName}),2) AS StatValue,
    Countif({PropertyName}>0) as Count,
    '{Description}' as Description,
    '{BaseStat}' AS BaseStat,
    '{ConditionCode}' AS ConditionCode,
    '{ObjectType}' AS ObjectType
  FROM
    `sportsight-tests.Baseball1.atBatPlays_pbp_enriched`
  WHERE
    {StatCondition}
    AND {PBPCondition}
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
