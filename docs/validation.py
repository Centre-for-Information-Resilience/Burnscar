# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "altair[all]==5.5.0",
#     "duckdb==1.2.2",
#     "marimo",
#     "polars==1.29.0",
#     "pyarrow==20.0.0",
#     "sqlglot==26.16.4",
#     "vega-datasets==0.9.0",
# ]
# ///

import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import altair as alt
    import marimo as mo
    import polars as pl

    return alt, mo, pl


@app.cell
def _():
    import duckdb

    DATABASE_URL = "sqlmesh/db.db"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    engine.install_extension("spatial")
    engine.load_extension("spatial")
    return (engine,)


@app.cell
def _(mo):
    old_df = mo.sql(
        """
        SELECT
            *, md5(concat(latitude::text, longitude::text)) as id
        FROM
            'output/validation/validated.csv'
        """
    )
    return (old_df,)


@app.cell
def _(mo, old_df):
    date_range = mo.sql(
        """
        select
            min(acq_date) + INTERVAL '1 day' as start_date,
            max(acq_date) - INTERVAL '1 day' as end_date
        from
            old_df
        """
    )
    return (date_range,)


@app.cell
def _(date_range, pl):
    date_range_filter = pl.col("acq_date").is_between(
        date_range["start_date"][0], date_range["end_date"][0]
    )
    return (date_range_filter,)


@app.cell
def _(engine, mo):
    table_names = mo.sql(
        """
        select name from (show all tables) where schema = 'burnscar'
        """,
        engine=engine,
    )
    return


@app.cell
def _(burnscar, engine, mo):
    firms_to_validate = mo.sql(
        """
        select firms_id, acq_date, st_y(st_geomfromwkb(geom)) as latitude, st_x(st_geomfromwkb(geom)) as longitude, md5(latitude::text || longitude::text) as id
        from burnscar.intermediate.firms_to_validate
        """,
        engine=engine,
    )
    return (firms_to_validate,)


@app.cell
def _(burnscar, engine, mo):
    firms = mo.sql(
        """
        select
            acq_date,
            st_y (geom) as latitude,
            st_x (geom) as longitude,
            md5(latitude::text || longitude::text) as id
        from
            burnscar.intermediate.firms
        """,
        engine=engine,
    )
    return (firms,)


@app.cell
def _(engine, mo):
    new_df = mo.sql(
        """
        SELECT
            *, md5(latitude::text || longitude::text) as id
        FROM
            'output/firms_output.csv'
        """,
        engine=engine,
    )
    return (new_df,)


@app.cell
def _(alt, date_range_filter, mo, new_df, old_df):
    counts = (
        old_df.filter(date_range_filter)
        .group_by("acq_date")
        .len("old")
        .join(new_df.group_by("acq_date").len("new"), on="acq_date")
        .unpivot(index="acq_date")
    )

    _chart = (
        alt.Chart(counts)
        .mark_bar()
        .encode(x="acq_date:O", y="value", color="variable", xOffset="variable")
    )

    chart = mo.ui.altair_chart(_chart)
    chart
    return


@app.cell
def _(date_range_filter, firms, new_df, old_df):
    old = old_df.filter(date_range_filter)
    new = new_df.filter(date_range_filter)

    old_rows = set(old["id"])
    new_rows = set(new["id"])

    diff = old_rows ^ new_rows
    miss = old_rows - new_rows

    print("Difference between old and new results:", len(diff))
    print(
        "Is the difference in results the same as what is missing from the new results?",
        diff == miss,
    )
    print(f"Percentage of old points covered: {len(new_rows) / len(old_rows):.1%}")
    print("Validated using old scripts:", len(old_rows))
    print("Validated using new scripts:", len(new_rows))
    print("Missing from raw FIRMS dataset:", len(old_rows - set(firms["id"])))
    print("Same as missing in new validation?", (old_rows - set(firms["id"])) == diff)
    return miss, new, old


@app.cell
def _(miss, old, pl):
    old.filter(pl.col("id").is_in(miss))
    return


@app.cell
def _(new, old):
    joined = old.join(new, on="id")
    joined
    return (joined,)


@app.cell
def _(joined, pl):
    comp = joined.with_columns(
        (pl.col("burn_scar_detected") == pl.col("burn_scar_detected_right")).alias(
            "burn_scar_match"
        ),
        (pl.col("no_data") == pl.col("no_data_right")).alias("no_data_match"),
        (pl.col("NAME_1") == pl.col("gadm_1")).alias("gadm_1_match"),
        (pl.col("NAME_2") == pl.col("gadm_2")).alias("gadm_2_match"),
        (pl.col("NAME_3") == pl.col("gadm_3")).alias("gadm_3_match"),
        (pl.col("urban_area_name") == pl.col("settlement_name")).alias(
            "settlement_name_match"
        ),
    )

    comp.select(pl.col("^*_match$"))
    return (comp,)


@app.cell
def _(comp, pl):
    comp.filter(pl.col("burn_scar_match") != True)[
        ["burn_scar_detected", "burn_scar_detected_right", "burnt_pixel_count"]
    ]
    return


@app.cell
def _(comp, pl):
    comp.filter(pl.col("no_data_match") != True)[
        ["no_data", "no_data_right", "burnt_pixel_count"]
    ]
    return


@app.cell
def _():
    return


@app.cell
def _(alt, new, old, pl):
    points = (
        old[["latitude", "longitude"]]
        .with_columns(pl.lit("old").alias("src"))
        .vstack(new[["latitude", "longitude"]].with_columns(pl.lit("new").alias("src")))
    )

    points_chart = (
        alt.Chart(points.to_pandas())
        .mark_point()
        .encode(
            longitude="longitude:Q",
            latitude="latitude:Q",
            size=alt.value(10),
            color="src",
        )
    )
    return (points_chart,)


@app.cell
def _(points_chart):
    points_chart
    return


if __name__ == "__main__":
    app.run()
