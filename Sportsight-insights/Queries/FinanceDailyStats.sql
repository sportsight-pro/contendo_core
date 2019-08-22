WITH
  base_data_filtered AS (
  SELECT
    base.*,
    sym.Type
  FROM
    `sportsight-tests.Finance_Data.eod_history_data_*` base
  LEFT JOIN
    `sportsight-tests.Finance_Data.eod_exchange_symbols_list` sym
  ON
    base.Code=sym.Code and base.Exchange=sym.Exchange
  WHERE
    _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) and FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
    AND (sym.Type in ('Common Stock', 'Commodity') or sym.Type='INDEX' and sym.Code in ('GSPC', 'DJI', 'VIX'))
  ),
  history_data AS (
  SELECT
    FORMAT('%s.%s', Code, Exchange) as Code,
    Code as Symbol,
    Name,
    Exchange,
    Type,
    MarketCapitalization as MarketCap,
    Date,
    if(open=0, NULL, open) as open,
    if(high=0, NULL, high) as high,
    if(low=0, NULL, low) as low,
    if(close=0, NULL, close) as close,
    if(Adjusted_close=0, NULL, Adjusted_close) as adjClose,
    volume
  FROM
    base_data_filtered),
  first_calc AS (
  SELECT
    *,  
    ROW_NUMBER() OVER (PARTITION BY Code ORDER BY Date DESC) AS rowNum,
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
  '{StatName}' AS StatName,
  {Stat} as StatValue
FROM
  second_calc
where Date >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))