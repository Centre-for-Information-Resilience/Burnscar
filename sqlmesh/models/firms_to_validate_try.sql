MODEL (
  name arson.firms_to_validate_@{try_},
  kind VIEW,
  description "FIRMS events pending a retry of validation.",
  blueprints ((try_ := 0, day_ := 11), (try_ := 1, day_ := 16), (try_ := 2, day_ := 21))
);

@DEF(validated_try, @EVAL(@try_ - 1));

SELECT
  *
FROM arson.firms_to_validate
WHERE
  @AND(
    (
      acq_date < CURRENT_DATE - INTERVAL (
        CONCAT(@day_, ' days')
      )
    ),
    @IF(
      @try_ > 0,
      (
        firms_id IN (
          SELECT DISTINCT
            firms_id
          FROM arson.firms_validated_@{validated_try}
          WHERE
            (
              no_data OR too_cloudy
            )
        )
      )
    )
  )