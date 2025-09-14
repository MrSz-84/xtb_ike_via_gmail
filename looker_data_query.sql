WITH
  calendar AS (
  SELECT
    d AS date
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2025-05-23', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 DAY)) AS d ),
  xtb_grouped AS (
  SELECT
    DATE(DataCzas) AS date,
    Symbol AS symbol,
    KlasaAktywow AS asset,
    Waluta AS currency,
    SUM(Wolumen) AS volume,
    SUM(CenaCalk) AS purchase_value,
    SUM(SAFE_MULTIPLY(CenaCalk, KursPrzewalutowania)) AS purchase_value_in_pln,
    SUM(LaczneKoszty) AS ttl_costs_in_pln,
    SUM(SAFE_DIVIDE(LaczneKoszty, KursPrzewalutowania)) AS ttl_costs,
    COUNT(*) AS transaction_count
  FROM
    `xtb-ike-wallet.retirement_portfolio.xtb_transactions_import`
  GROUP BY 1, 2, 3, 4 ),
  grid AS (
  SELECT
    c.date,
    s.symbol,
    s.currency,
    s.asset
  FROM
    calendar AS c
  CROSS JOIN (
    SELECT
      DISTINCT Symbol, Waluta AS currency, KlasaAktywow AS asset
    FROM
      `xtb-ike-wallet.retirement_portfolio.xtb_transactions_import` ) AS s ),
  all_data AS (
  SELECT
    grid.date,
    grid.symbol,
    grid.currency,
    grid.asset,
    xtb.volume,
    xtb.purchase_value,
    xtb.purchase_value_in_pln,
    xtb.ttl_costs,
    xtb.ttl_costs_in_pln,
    xtb.transaction_count,
    equity.close AS eq_close_value,
    nbp.mid AS nbp_mid_ex_rate,
    fx.close AS fx_close_rate
  FROM
    grid
  LEFT JOIN
    xtb_grouped AS xtb
  ON
    grid.date = xtb.date AND grid.symbol = xtb.symbol
  LEFT JOIN
    `xtb-ike-wallet.retirement_portfolio.alpha_equity_data` AS equity
  ON
    grid.date = equity.date AND grid.symbol = equity.symbol
  LEFT JOIN
    `xtb-ike-wallet.retirement_portfolio.nbp_usdpln_exchange_rates` AS nbp
  ON
    grid.date = nbp.Date AND grid.currency = nbp.Currency
  LEFT JOIN
    `xtb-ike-wallet.retirement_portfolio.alpha_fx_data` AS fx
  ON
    grid.date = fx.date AND grid.currency = fx.from),
  pre_final AS (
  SELECT
    *,
    SUM(IFNULL(volume, 0)) OVER (PARTITION BY symbol, currency, asset ORDER BY date) AS running_volume,
    SUM(IFNULL(purchase_value, 0)) OVER (PARTITION BY symbol, currency, asset ORDER BY date) AS running_purchase_value,
    SUM(IFNULL(purchase_value_in_pln, 0)) OVER (PARTITION BY symbol, currency, asset ORDER BY date) AS running_purchase_value_in_pln,
    SUM(IFNULL(ttl_costs, 0)) OVER (PARTITION BY symbol, currency, asset ORDER BY date) AS running_costs,
    SUM(IFNULL(ttl_costs_in_pln, 0)) OVER (PARTITION BY symbol, currency, asset ORDER BY date) AS running_costs_in_pln,
    SUM(IFNULL(transaction_count, 0)) OVER (PARTITION BY symbol, currency, asset ORDER BY date) AS running_transaction_count,
    LAST_VALUE(eq_close_value IGNORE NULLS) OVER (PARTITION BY symbol ORDER BY date) AS close_fwd_filled,
    LAST_VALUE(nbp_mid_ex_rate IGNORE NULLS) OVER (PARTITION BY currency ORDER BY date) AS mid_ex_rate,
    LAST_VALUE(fx_close_rate IGNORE NULLS) OVER (PARTITION BY currency ORDER BY date) AS close_fwd_filled_fx,
    date = MAX(date) OVER (PARTITION BY DATE_TRUNC(date, ISOWEEK)) AS is_last_date_in_week,
    date = MAX(date) OVER (PARTITION BY DATE_TRUNC(date, MONTH)) AS is_last_date_in_month,
    date = MAX(date) OVER (PARTITION BY DATE_TRUNC(date, QUARTER)) AS is_last_date_in_quarter,
    date = MAX(date) OVER () AS is_last_available_date
  FROM
    all_data),
  calculated AS (
  SELECT
    *,
    SAFE_MULTIPLY(running_volume, close_fwd_filled) AS running_current_value,
    SAFE_MULTIPLY(SAFE_MULTIPLY(running_volume, close_fwd_filled), mid_ex_rate) AS running_current_value_in_pln,
    SAFE_MULTIPLY(SAFE_MULTIPLY(running_volume, close_fwd_filled), close_fwd_filled_fx) AS running_current_fx_value_in_pln
  FROM pre_final),
  sum_by_date AS (
  SELECT
    date,
    SUM(running_current_value_in_pln) AS running_current_nbp_value_in_pln,
    SUM(running_current_fx_value_in_pln) AS running_current_fx_value_in_pln
  FROM
    calculated
  GROUP BY
    date ),
  final_calculated AS (
  SELECT
    calculated.*,
    SAFE_DIVIDE((sum_by_date.running_current_nbp_value_in_pln - MAX(sum_by_date.running_current_nbp_value_in_pln) OVER (ORDER BY calculated.date)), MAX(sum_by_date.running_current_nbp_value_in_pln) OVER (ORDER BY calculated.date)) AS all_pln_nbp_daily_drawdown,
    SAFE_DIVIDE((sum_by_date.running_current_fx_value_in_pln - MAX(sum_by_date.running_current_fx_value_in_pln) OVER (ORDER BY calculated.date)), MAX(sum_by_date.running_current_fx_value_in_pln) OVER (ORDER BY calculated.date)) AS all_pln_fx_daily_drawdown,
    SAFE_DIVIDE((calculated.running_current_value_in_pln - MAX(calculated.running_current_value_in_pln) OVER (PARTITION BY calculated.symbol ORDER BY calculated.date)), MAX(calculated.running_current_value_in_pln) OVER (PARTITION BY calculated.symbol ORDER BY calculated.date)) AS pln_nbp_daily_drawdown,
    SAFE_DIVIDE((calculated.running_current_fx_value_in_pln - MAX(calculated.running_current_fx_value_in_pln) OVER (PARTITION BY calculated.symbol ORDER BY calculated.date)), MAX(calculated.running_current_fx_value_in_pln) OVER (PARTITION BY calculated.symbol ORDER BY calculated.date)) AS pln_fx_daily_drawdown
  FROM
    calculated
  LEFT JOIN sum_by_date ON sum_by_date.date = calculated.date
)
SELECT
  date,
  symbol,
  currency,
  asset,
  volume,
  running_volume,
  purchase_value,
  running_purchase_value,
  purchase_value_in_pln,
  running_purchase_value_in_pln,
  ttl_costs,
  running_costs,
  ttl_costs_in_pln,
  running_costs_in_pln,
  transaction_count,
  running_transaction_count,
  close_fwd_filled AS eq_close_value,
  SAFE_MULTIPLY(volume, close_fwd_filled) AS current_value,
  running_current_value,
  mid_ex_rate,
  SAFE_MULTIPLY(SAFE_MULTIPLY(volume, close_fwd_filled), mid_ex_rate) AS current_value_in_pln,
  running_current_value_in_pln,
  is_last_date_in_week,
  is_last_date_in_month,
  is_last_date_in_quarter,
  is_last_available_date,
  close_fwd_filled_fx AS fx_ex_close_rate,
  running_current_fx_value_in_pln,
  all_pln_nbp_daily_drawdown,
  all_pln_fx_daily_drawdown,
  pln_nbp_daily_drawdown,
  pln_fx_daily_drawdown
FROM
  final_calculated
ORDER BY date ASC;
