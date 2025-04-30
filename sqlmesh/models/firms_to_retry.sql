MODEL (
  name arson.firms_to_retry,
  kind VIEW,
  description "FIRMS events pending a retry of validation."
);

WITH firms_failed AS (
  SELECT DISTINCT
    firms_id
  FROM arson.firms_validated_try_1
  WHERE
    (
      no_data OR too_cloudy
    )
    AND acq_date < CURRENT_DATE - INTERVAL (
      CONCAT(@analyse_retry_after_days, ' days')
    )
)
SELECT
  *
FROM arson.firms_to_validate
WHERE
  firms_id IN (
    SELECT
      firms_id
    FROM firms_failed
  )