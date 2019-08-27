WITH
  base_data_filtered AS (
  SELECT
    base.*,
    sym.Type,
    sym.Name,
    fundamentals.General.Sector,
    fundamentals.Highlights.MarketCapitalizationMln AS MarketCap
  FROM
    `sportsight-tests.Finance_Data.eod_daily_history_2019` base
  LEFT JOIN
    `sportsight-tests.Finance_Data.eod_exchange_symbols_list` sym
  ON
    base.Symbol=sym.Code and base.Exchange=sym.Exchange
  LEFT JOIN
    `sportsight-tests.Finance_Data.fundamentals_daily_` fundamentals
  ON
    base.Symbol=fundamentals.General.Code and base.Exchange=fundamentals.General.Exchange

  WHERE
    #_TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) and FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
    Date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) and DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
    AND ((sym.Type in ('Common Stock', 'Commodity') or sym.Type='INDEX' and sym.Code in ('GSPC', 'DJI', 'VIX')))
  ),
  history_data AS (
  SELECT
    '{StatName}' AS StatName,
    FORMAT('%s.%s', Symbol, Exchange) as Code,
    Symbol,
    Name,
    Sector,
    Exchange,
    Type,
    MarketCap,
    Date,
    if(open=0, NULL, open) as open,
    if(high=0, NULL, high) as high,
    if(low=0, NULL, low) as low,
    if(close=0, NULL, close) as close,
    if(Adjusted_close=0, NULL, Adjusted_close) as adjClose,
    volume
  FROM
    base_data_filtered),
  ytd AS (
  SELECT
    FIRST_VALUE(adjClose) OVER (PARTITION BY code ORDER BY date) ytdClose,
    code,
    date
  FROM
    history_data hd
  WHERE
    Date>='2018-12-31'
    and adjClose IS NOT NULL),
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
  StatName
  {Stat} as StatValue,
  DENSE_RANK() OVER (PARTITION BY StatName ORDER BY StatName DESC) AS DenseRank
FROM
  second_calc
where
  Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
  AND Date=lastDate
  AND ({StatCondition})