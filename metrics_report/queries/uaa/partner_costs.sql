-- Partner cost breakdown for the latest available run_date in cost_cube.
-- Using MAX(run_date) instead of CURRENT_DATE - 1 ensures we always surface
-- real data even when the cost pipeline is delayed by a day or more.
WITH latest AS (
    SELECT MAX(DATE(run_date)) AS latest_date
    FROM iceberg_db.cost_cube
    WHERE partner IN ('TU_CR', 'PLAID', 'EFX_IIG', 'TU_CCS', 'EFX_EPAY', 'EVOLVE', 'TELLER', 'EFX_CDS', 'FISERV')
)
SELECT
    c.partner,
    ROUND(SUM(CASE WHEN c.billing_type = 'ONE_TIME' THEN c.cost ELSE 0 END), 2) AS one_time_cost,
    ROUND(SUM(CASE WHEN c.billing_type = 'MONTHLY'  THEN c.cost ELSE 0 END), 2) AS maintenance_cost,
    ROUND(SUM(c.cost), 2)                                                        AS daily_cost,
    l.latest_date                                                                AS run_date
FROM iceberg_db.cost_cube c
CROSS JOIN latest l
WHERE c.partner IN ('TU_CR', 'PLAID', 'EFX_IIG', 'TU_CCS', 'EFX_EPAY', 'EVOLVE', 'TELLER', 'EFX_CDS', 'FISERV')
  AND DATE(c.run_date) = l.latest_date
GROUP BY c.partner, l.latest_date
ORDER BY c.partner
