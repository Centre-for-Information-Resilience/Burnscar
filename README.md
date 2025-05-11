# Fire Detection and Mapping


Here's the updated `README.md`, now including:

* Requirements for the NASA FIRMS API key and Google Earth Engine project access
* Badges for build status (placeholder), license, and Python version

---

# 🔥 Arson Detection Pipeline

[![Build](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/your-repo)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

This project identifies potential arson incidents using NASA FIRMS active fire alerts, filtered and validated with post-event burn scar imagery from Copernicus Sentinel-2. It uses geospatial joins, temporal validation, and clustering to produce a reliable dataset of suspicious fires. The data pipeline is powered by **[SQLMesh](https://sqlmesh.com/)**.

---

## 🧠 Pipeline Summary

1. **Fetch** fire alerts from NASA FIRMS.
2. **Filter** alerts spatially using GADM boundaries.
3. **Validate** detections via Sentinel-2 imagery (Google Earth Engine).
4. **Cluster** validated points by date and proximity to urban areas.
5. **Export** clean datasets of verified and clustered fires.

---

## 📦 Requirements

Before you begin:

- **NASA FIRMS API key**: [Get it here](https://firms.modaps.eosdis.nasa.gov/api/map_key/)
- **Google Earth Engine access**: [Request access](https://developers.google.com/earth-engine/guides/access)
- [uv](https://github.com/astral-sh/uv) (for dependency management)

---

## 🚀 Quickstart

### 1. Clone and install dependencies

```bash
uv sync
````

### 2. Add credentials

To set your API keys, change the `example.env` file to `.env` and fill in your API keys for FIRMS and Google Earth Engine.

For configuration of the project refer to the SQLMesh [config file](./sqlmesh/config.yaml). The following configuration options are currently available:

- `model_defaults.start`: Set the start date of the project.
- `variables`
    - `ee_project`: Name of the Google Earth Engine project
    - `ee_concurrency`: Max number of threads used for fetching data from gee. 50 uses ~1.5GB of RAM
    - `country_id`: 3-letter country code
    - `gadm_level`: GADM administrative areas level (between 1 and 3). Some countries don't have higher levels available

    - `validation_lookback`: How many days back to look when running the pipeline. e.g. 60 will fetch and validate fires up to 60 days ago

    - # validation parameters
    - `validation_params`:
        - `buffer_distance`: Area in meters around fire to use for validation TODO: Check whether this is actually in meters.
        - `days_around`: Days before and after the event to consider for validation
        - `max_cloudy_percentage`: Maximum allowed cloud cover for images used in validation
        - `burnt_pixel_count_threshold`: Required amount of burnt pixels to label as `burn_scar_detected`
        - `nbr_after_lte`: -0.10
        - `nbr_difference_limit`: 0.15

    - # clustering
    - `clustering_max_date_gap`: 2

    - # paths
    - `path_raw_data`: ../data/raw
    - `path_geo`: ../geo
    - `path_gadm`: ../data/gadm
    - `path_settlements`: ../geo/settlements.gpkg
    - `path_output`: ../output

    - `paths_areas`:
        - `include`: ../geo/include.gpkg
        - `exclude`: ../geo/exclude.gpkg


### 3. Run the SQLMesh pipeline

```bash
cd sqlmesh
uv run sqlmesh run
```

### 4. Inspect the results

- Outputs are saved in the `output/` folder:
  - `firms_output.csv`: all validated fire detections
  - `output_clustered.csv`: clustered detections by date and location
- You can explore the `sqlmesh/db.db` database with:
  - [DuckDB CLI](https://duckdb.org/2025/03/12/duckdb-ui.html): `duckdb sqlmesh/db.db -ui`
  - [marimo](https://marimo.io): `uvx marimo edit explore.py --sandbox`

---

## 📂 Project Structure

```text
data/               # GADM admin boundary data
geo/                # Custom spatial boundaries and settlements
sqlmesh/            # SQLMesh config, models, macros
src/arson_analyser/ # Python source (fetchers, validators, logic)
output/             # Final exported datasets
logs/               # Pipeline logs
docs/               # Validation scripts & diagrams
```

---

## 🗺️ Data Sources

* **FIRMS** – NASA MODIS/VIIRS fire alerts
* **Copernicus Sentinel-2** – Post-burn surface reflectance
* **GADM** – Global administrative boundaries
* **Urban settlements layer** – Custom vector for clustering

---

## 📜 License

MIT License — free to use, modify, and distribute.



# Old Readme
> ### Goals
> This is a work in progress proejct that is aimed at refactoring a number of different components. 

> - Minimum objective is to make this easier to run and to better manage dependencies.
> - Ideal outcome is to have this deployable.

> ## How to run
> ### Data Fetcher
> Run DataFetcher (gets FIRMS data points from FIRMS API. You first need to insert a map key in line 9).
> The API allows download in batches of 10 days, the script therefore uses 9 days increments. You need to put the start date in line 22, (I always put the current or last date of the period I'm using to triage – see flow chart – in a note in that line for the next time I'm running this script
> To prevent FIRMS rate limiting or blocking the map key, the scripts sleeps in between increments, so it takes a while.
> For my own convenience, I've added a line print(f"Currently checking for date {current_date}") in the While loop at the end, so that I can see where the script currently is. Note that "current date" means a period of 10 days up to that date. (If this line does not already exists in the shared script) 
> Note that the script continues even after reaching the current day. I stop running as soon the script returns no new entries/current date is 10 days over the last date I want to include in the period.

> ### Arson analyser
> Feed the CSV output from step 1 to Arson_Analyser_Latest_update 
> There's 3 things you need to do before you can run this script: 
> - Create a Google Earth Engine project name in line 70, you can create a project here, 
> - Edit the base directory in line 16
> - Have the data.csv output from the first step ready in the correct folder, see line 17. If you want to know a bit about what the script and GEE do, you can read a summary of the methodology here

> Insert the CSV output from step 2 in Google Sheets as a new tab into the fire sheet for analysis and triage:
> - Rename tab, e.g. 'Jan_16' (Jan_16 from here on)
> - Sort column 'burn_scar_detected' from Z to A
> - Check if there are any cities with the same unique event number in 'unique_event_no'. If yes -> change unique event no so that these entries won't get deleted in next step
> - Remove duplicate entries (entries that were created because of multiple detections at same location, columns to analyze: D - Name_3 / E Urban area /I Unique event number)
> - Check the entries with FALSE in burn_scar_detected for entries with a 'high risk' location (area with recent combat activity/reported conflict around the date/location)
> - Put the name of Jan_16 in cell S2 of the 'Conversion' tab (this will reformat the entries so they can be easily copy-pasted into the main tab)
> - Copy-paste the entries from the 'Conversion' tab that have 'TRUE', and optionally the entries identified in step 3e, in the 'burn_scar_detected' column to the 'Main' tab of the fire sheet
> - Discard the entries in Jan_16 that were copy-pasted to the main tab
> - Keep the remaining 'too cloudy' and 'no image' entries in the tab for the next run

> [Next run, minimal 5 days later] Second run for the 'too cloudy' and 'no image' entries (this process is shown by the red dotted lines in the flow chart)
> - Run the DataFetcher (see step 1) for the period that covers all remaining entries from 3i
> - Filter the DataFetcher output so that only detections remain that are also in the list with remaining entries from 3i (I do this in Google Sheets, I can explain this if you want me to)
> - Feed the filtered csv from step b to the Arson Analyse (step 2)
> - Follow the steps under 3 for the output, with the exception of step 3i (as this is the second time results come back as no image/too cloudy). Keep the remaining data for step 5.

> Manually analyse the remaining point on a map
> - Do the verification right away where possible
> - Discard locations that are low risk
> - Add everything else to the fire datasheet for the human verification

> Human verification in the final sheet, completing entries and reviewing before 

> ## Suggested priorities:

> Prio's 1a,b,c are related and should be ordered or merged if more convenient, but if I have to order them, then prio 1a would have the highest priority, and then b,c. This is in lesser extent also true for prio 2 and 3. Important note: It seems that the Arson Analyser stops checking for new Sentinel-2 imagery after there's no good imagery available for more than a number of days after the incident. I'll double check this with Micheal/Tarig, who have been working on both scripts. It would be good to know if it makes sense to extent this period for a bit, as I now still get more than 50% back with no image in the second run.

> - Prio 1a: Edit the Arson Analyser script so that there is a second output with all detections that come back as 'too cloud' or 'no image' in the same format as the input (saves step 4a-4c)
> - Prio 1b: Edit the Arson Analyser script so that it also works through the second output from prio 1a on request, while still having an output that distinguishes between these and the 'new' detections
> - Prio 1c: Merge the DataFetcher script with the Arson Analyser so that you only have to run one script instead of two, preferably by selecting a specific period you want to check (merges steps 1 and 2)
> - Prio 2a: Remove duplicate detections within the same boundary (if this is done before the process in GEE, this saves time/resources as well). The downside is that we then have to check for other detections within the same boundary on the same day, but I think this is more efficient. (saves step 3d)
> - Prio 2b: Merge (preferred way, e.g. by adding a column with number of days) and/or remove duplicate detections within the same boundary on subsequent days (saves time in the final verification process)
> - Prio 3: Edit the Arson Analyser script so that the first columns of the output are the same as column E-P (with column E on "In Progress" and F-J empty) in the 'Main' tab of the fire sheet (saves step 3f) 
> Anything else flagged by Lewis after reading through the steps and looking at scripts etc.

> Other potential improvements: 
> - Incorporate controlled list of village names 
> - Put damage assessment numbers back in
> - Use Planet as backup if S-2 fails (would be a major improvement, but also investment in development, quota)
