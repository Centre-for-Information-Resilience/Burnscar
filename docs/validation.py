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
def _(mo):
    old_df = mo.sql(
        f"""
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
        f"""
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
    date_range_filter = pl.col("acq_date").is_between(date_range["start_date"][0], date_range["end_date"][0])
    return (date_range_filter,)


@app.cell
def _(mo):
    new_df = mo.sql(
        f"""
        SELECT
            *, md5(concat(latitude::text, longitude::text)) as id
        FROM
            'output/firms_output.csv'
        """
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

    _chart = alt.Chart(counts).mark_bar().encode(x="acq_date:O", y="value", color="variable", xOffset="variable")

    chart = mo.ui.altair_chart(_chart)
    chart
    return


@app.cell
def _(date_range_filter, new_df, old_df):
    old = old_df.filter(date_range_filter)
    new = new_df.filter(date_range_filter)

    old_rows = set(old["id"])
    new_rows = set(new["id"])

    diff = old_rows ^ new_rows
    miss = old_rows - new_rows

    diff == miss
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
        (pl.col("burn_scar_detected") == pl.col("burn_scar_detected_right")).alias("burn_scar_match"),
        (pl.col("no_data") == pl.col("no_data_right")).alias("no_data_match"),
        (pl.col("NAME_1") == pl.col("gadm_1")).alias("gadm_1_match"),
        (pl.col("NAME_2") == pl.col("gadm_2")).alias("gadm_2_match"),
        (pl.col("NAME_3") == pl.col("gadm_3")).alias("gadm_3_match"),
        (pl.col("urban_area_name") == pl.col("settlement_name")).alias("settlement_name_match"),
    )

    comp.select(pl.col("^*_match$"))
    return (comp,)


@app.cell
def _(comp, pl):
    comp.filter(pl.col("burn_scar_match") != True)[["burn_scar_detected", "burn_scar_detected_right", "burnt_pixel_count"]]
    return


@app.cell
def _(comp, pl):
    comp.filter(pl.col("no_data_match") != True)[["no_data", "no_data_right", "burnt_pixel_count"]]
    return


@app.cell
def _(comp, pl):
    comp.filter(pl.col("gadm_1_match") != True)[["NAME_1", "gadm_1"]]
    return


@app.cell
def _(comp, pl):
    comp.filter(pl.col("gadm_2_match") != True)[["NAME_2", "gadm_2"]]
    return


@app.cell
def _(comp, pl):
    comp.filter(pl.col("gadm_3_match") != True)[["NAME_3", "gadm_3"]]
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
        .interactive()
    )
    return (points_chart,)


@app.cell
def _(mo, points_chart):
    mo.ui.altair_chart(points_chart)
    return


if __name__ == "__main__":
    app.run()
