WITH
  base_data_filtered AS (
  SELECT
    base.Symbol,
    sym.Name,
    FORMAT('%s.%s', base.Symbol, base.Exchange) as Code,
    sym.Type,
    comp.Sector,
    comp.TrendValue,
    comp.isDJI,
    comp.isSNP,
    base.Exchange,
    base.Date,
    if(open=0, NULL, open) as open,
    if(high=0, NULL, high) as high,
    if(low=0, NULL, low) as low,
    if(close=0, NULL, close) as close,
    if(Adjusted_close=0, NULL, Adjusted_close) as adjClose,
    volume,
    comp.MarketCapitalizationMln AS MarketCap
  FROM
    `sportsight-tests.Finance_Data.eod_daily_history_1year` base
  LEFT JOIN
    `sportsight-tests.Finance_Data.eod_exchange_symbols_list` sym
  ON
    base.Symbol=sym.Code and base.Exchange=sym.Exchange
  LEFT JOIN
    `sportsight-tests.Finance_Data.all_company_data` comp
  ON
    base.Symbol=comp.Symbol and base.Exchange=comp.Exchange

  WHERE
    TRUE
    #_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) and FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
    #AND Date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) and DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
    AND ((sym.Type in ('Common Stock', 'Commodity') or sym.Type='INDEX' and sym.Code in ('GSPC', 'DJI', 'VIX')))
  ),
  history_data AS (
  SELECT
    '{StatName}' AS StatName,
    *,
    LAG(adjClose) OVER (PARTITION BY code ORDER BY date) prevAdjClose
    #if (LAG(adjClose) <= adjClose, 0, 1) OVER (PARTITION BY code ORDER BY date) isUp,
    #if (LAG(adjClose) >= adjClose, 0, 1) OVER (PARTITION BY code ORDER BY date) isDown
  FROM
    base_data_filtered),
  first_close_since AS (
  SELECT
    FIRST_VALUE(prevAdjClose) OVER (PARTITION BY code ORDER BY date) firstClose,
    code,
    date
  FROM
    history_data hd
  WHERE
    TRUE
    AND {DateCondition}
    AND adjClose IS NOT NULL),
  first_calc AS (
  SELECT
    *,  
    ROW_NUMBER() OVER (PARTITION BY Code ORDER BY Date DESC) AS rowNum,
    FIRST_VALUE(Date) OVER (PARTITION BY Code ORDER BY Date DESC) AS lastDate,
    ROUND(AVG(adjClose) OVER (PARTITION BY Code ORDER BY Date ASC ROWS BETWEEN {RollingDays} PRECEDING AND 0 PRECEDING), 3) AS closeAVG,
    ROUND(STDDEV_POP(adjClose) OVER (PARTITION BY Code ORDER BY Date ASC ROWS BETWEEN {RollingDays} PRECEDING AND 0 PRECEDING), 3) AS closeSTD,
    ROUND(SUM(volume) OVER (PARTITION BY Code ORDER BY Date ASC ROWS BETWEEN {RollingDays} PRECEDING AND 0 PRECEDING), 3) AS volumeSum,
    {CalcStage1}
  FROM
    history_data
  ),
  second_calc AS (
  SELECT
    *,
    {CalcStage2}
  FROM
    first_calc
  )
SELECT
  '{StatObject}' AS StatObject,
  '{RollingDays}' as StatRollingDays,
  Code,
  Symbol,
  Sector,
  Exchange,
  isDJI,
  isSNP,
  Type,
  Name,
  MarketCap,
  Date as StatDate,
  open,
  high,
  low,
  close,
  adjClose,
  volume,
  StatName,
  FORMAT('%s (%s)', Name, Symbol) AS ObjectName,
  {Stat} as StatValue,
  DENSE_RANK() OVER (PARTITION BY StatName ORDER BY StatName DESC) AS DenseRank
FROM
  second_calc
where
  Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
  AND Date=lastDate
  AND ({StatCondition})