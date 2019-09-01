WITH
  insightsConfiguration AS (
  SELECT
    QuestionCode,
    StatName AS StatCode,
    StatObject AS StatObjects,
    RollingDays,
    SentenceRegardingList,
    ListDescription,
    Value1Template
  FROM
    `{InsightsConfigurationTable}`
  WHERE
    StatName IS NOT NULL
    and doit='y'
    ),
  insightStats AS (
  SELECT
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY StatValue DESC) AS highDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY StatValue DESC) AS highRank,
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY StatValue ASC) AS lowDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, QuestionCode ORDER BY StatValue ASC) AS lowRank,
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
    AND STRPOS(StatObjects, StatObject) > 0
    AND STRPOS(RollingDays, StatRollingDays) > 0
  ),
  insightStatsFinal AS (
  SELECT
    '{ContentDefCode}' as ContentDefCode,
    FORMAT('%s %s', REPLACE(SentenceRegardingList, '{RollingDays}', StatRollingDays), ListDescription) as ListTitle,
    if (highRank<={ListSize}, 'TOP', 'BOTTOM') as TopBottom,
    *
  FROM
    insightStats
  WHERE
    (highRank<={ListSize} or lowRank<={ListSize})
    AND StatDate=maxDate
  )
select * from insightStatsFinal
