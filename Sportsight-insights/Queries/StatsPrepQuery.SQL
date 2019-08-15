WITH
  insightsConfiguration AS (
  SELECT
    QuestionCode,
    StatName AS StatCode,
    StatObject AS StatObjects,
    SeasonCodes,
    StatTimeframes,
    Question2Objects,
    startSlot,
    Value1Template,
    Value2Template
  FROM
    `{InsightsConfigurationTable}`
  WHERE
    StatName IS NOT NULL
    and doit='y'
    ),
  insightStats AS (
  SELECT
    DENSE_RANK() OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode ORDER BY StatValue DESC) AS internalDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode ORDER BY StatValue DESC) AS internalRank,
    COUNT(*) OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode) AS objectsCount,
    MIN(StatValue) OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode) AS minValue,
    MAX(StatValue) OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode) AS maxValue,
    CASE
      WHEN StatObject='team' THEN TeamName
      WHEN StatObject='player' THEN FORMAT("%s (%s)", PlayerName, TeamName)
      WHEN SportCode='Entertainmant' THEN PlayerName
    END
    AS StatObjectName,
    insightsConfiguration.*,
    stats.*
  FROM
    `Sportsight_Stats.all_stats` stats
  LEFT JOIN
    insightsConfiguration
  ON
    StatName=StatCode
  WHERE
    TRUE
    AND {StatFilter}
    AND TeamCode IS NOT NULL
    AND PlayerCode IS NOT NULL
    AND StatValue>0
    AND STRPOS(StatTimeframes, StatTimeframe) > 0
    AND STRPOS(StatObjects, StatObject) > 0
    AND STRPOS(SeasonCodes, SeasonCode) > 0 ),
  insightStats_valrange AS (
  SELECT
    COUNT(*) OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode, internalDenseRank) AS rankItemsCount,
    ABS(maxValue-minValue) AS valueRange,
    *
  FROM
    insightStats
  WHERE
    maxValue-minValue>0
    AND internalRank < {MaxItems}
    ),
  insightStats_numranks AS (
  SELECT
    MAX(internalDenseRank) OVER (PARTITION BY StatObject, StatTimeframe, LeagueCode, SeasonCode, CompetitionStageCode, GamePeriodCode, QuestionCode) AS numRanks,
    *
  FROM
    insightStats_valrange),
  insightStats_filtered AS (
  SELECT
    *
  FROM
    insightStats_numranks
  WHERE
    #
    # Filter the low 80% if there are small number of ranks or small number of objects
    internalDenseRank <= IF (numRanks>=5 AND objectsCount>=40, numRanks*0.8, numRanks)
  ),
  #
  # Final query in this file - should always be last.
  insightStatsFinal AS (
  SELECT
    *
  FROM
    insightStats_numranks
  WHERE
    numRanks>=2)
