MODEL (
  name arson.firms_validated,
  kind VIEW,
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);


select * from (@UNION('DISTINCT', arson.firms_validated_try_1, arson.firms_validated_try_2))
ORDER BY acq_date