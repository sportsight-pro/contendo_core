WITH
  insightStats AS (
  SELECT
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue DESC) AS highDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue DESC) AS TopRank,
    DENSE_RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue ASC) AS lowDenseRank,
    RANK() OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName ORDER BY StatValue ASC) AS BottomRank,
    COUNT(*) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName) AS objectsCount,
    MIN(StatValue) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName) AS minValue,
    MAX(StatValue) OVER (PARTITION BY StatObject, StatRollingDays, StatDate, StatName) AS maxValue,
    MAX(StatDate) OVER (PARTITION BY StatObject, StatRollingDays, StatName) AS maxDate,
    CASE
      WHEN StatObject='Sector' THEN Sector
      WHEN StatObject='Stock' THEN FORMAT("%s (%s)", Name, Symbol)
    END
    AS StatObjectName,
    stats.*
  FROM
    `Finance_Data.all_stats_finance` stats
  WHERE
    StatName = '{StatName}'
    AND {SectorCondition}
    AND {MarketCapCondition}
    AND ({DJICondition} OR {SNPCondition})
    #AND {StatObjectCondition}
    AND {RollingDaysCondition}
  ),
  insightStatsFinal AS (
  SELECT
    if (TopRank<={ListSize}, 'TOP', 'BOTTOM') as TopBottom,
    *
  FROM
    insightStats
  WHERE
    (TopRank<={ListSize} or BottomRank<={ListSize})
    AND StatDate=maxDate
  )
SELECT
  StatName,
  Symbol,
  Name,
  Sector,
  Exchange,
  isDJI,
  isSNP,
  MarketCap,
  format_date('%Y-%m-%d', StatDate) as StatDate,
  StatValue,
  TopBottom,
  TopRank,
  BottomRank
FROM
  insightStatsFinal
ORDER BY
  TopBottom DESC,
  TopRank