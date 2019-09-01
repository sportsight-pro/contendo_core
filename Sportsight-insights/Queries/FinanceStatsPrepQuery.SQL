WITH
  insightsConfiguration AS (
  SELECT
    QuestionCode,
    StatName AS StatCode,
    StatObject AS StatObjects,
    RollingDays,
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
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY StatValue DESC) AS internalDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY StatValue DESC) AS internalRank,
    COUNT(*) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode) AS objectsCount,
    MIN(StatValue) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode) AS minValue,
    MAX(StatValue) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode) AS maxValue,
    MAX(StatDate) OVER (PARTITION BY StatObject, StatRollingDays, QuestionCode) AS maxDate,
    CASE
      WHEN StatObject='Sector' THEN Sector
      WHEN StatObject='Stock' THEN FORMAT("%s (%s)", Name, Symbol)
    END
    AS StatObjectName,
    insightsConfiguration.*,
    stats.*
  FROM
    `Finance_Data.all_stats_finance` stats
  LEFT JOIN
    insightsConfiguration
  ON
    StatName=StatCode
  WHERE
    TRUE
    AND {StatFilter}
    #AND StatValue>0
    AND STRPOS(StatObjects, StatObject) > 0
    AND STRPOS(RollingDays, StatRollingDays) > 0
  ),
  insightStats_valrange AS (
  SELECT
    COUNT(*) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode, internalDenseRank) AS rankItemsCount,
    ABS(maxValue-minValue) AS valueRange,
    *
  FROM
    insightStats
  WHERE
    maxValue-minValue>0
    AND StatDate=maxDate
    AND internalRank < {MaxItems}
    ),
  insightStats_numranks AS (
  SELECT
    MAX(internalDenseRank) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode) AS numRanks,
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
