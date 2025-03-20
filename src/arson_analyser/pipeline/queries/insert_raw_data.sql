INSERT INTO raw_data (
                SELECT *
                FROM read_parquet(
                                $path_raw_data,
                                filename = false
                        )
                WHERE acq_date NOT IN (
                                SELECT DISTINCT acq_date
                                FROM raw_data
                        )
        )