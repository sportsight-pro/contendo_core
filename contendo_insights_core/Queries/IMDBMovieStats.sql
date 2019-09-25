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
    '{Genre}' AS LeagueCode,
    'All' AS SeasonCode,
    'N/A' as CompetitionStageCode,
    'N/A' as CompetitionDay,
    'N/A' AS GameCode,
    'N/A' as GamePeriodCode,
    cast(startYear as string) AS TeamCode,
    tconst AS PlayerCode,
    '{StatName}' AS StatName,
    ROUND({Stat},2) AS StatValue,
    MAX(imdb_numvotes) as Count,
    '{Description}' as Description
  FROM
    `sportsight-tests.temps.titles_view_v1`
    WHERE
      TRUE
      {StatCondition}
      AND titleType='{TitleType}'
      AND imdb_numvotes >= {minVotes}
  GROUP BY
    LeagueCode,
    SeasonCode,
    CompetitionStageCode,
    CompetitionDay,
    GameCode,
    GamePeriodCode,
    teamCode,
    playerCode,
    StatName,
    StatValue
    ) WHERE StatValue is not NULL and StatValue > 0
