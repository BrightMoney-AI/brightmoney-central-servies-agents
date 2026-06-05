SELECT
    partner,
    ROUND(SUM(CASE WHEN billing_type = 'ONE_TIME' THEN cost ELSE 0 END), 2) AS daily_cost,
    ROUND(SUM(CASE WHEN billing_type = 'MONTHLY'  THEN cost ELSE 0 END), 2) AS maintenance_cost
FROM iceberg_db.cost_cube
WHERE partner IN ('TU_CR', 'PLAID', 'EFX_IIG', 'TU_CCS', 'EFX_EPAY', 'EVOLVE', 'TELLER', 'EFX_CDS', 'FISERV')
  AND DATE(run_date) = CURRENT_DATE
GROUP BY partner
ORDER BY partner
