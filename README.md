# 🔥 Burnscar Detection Pipeline

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

This project identifies potential arson incidents using NASA FIRMS active fire alerts, filtered and validated with post-event burn scar imagery from Copernicus Sentinel-2. It uses geospatial joins, temporal validation, and clustering to produce a reliable dataset of suspicious fires. The data pipeline is powered by **[SQLMesh](https://sqlmesh.com/)**.

---

## 🧠 Pipeline Summary

1. **Fetch** fire alerts from NASA FIRMS.
2. **Filter** alerts spatially using spatial filter. (usually urban areas)
3. **Validate** detections via Sentinel-2 imagery (Google Earth Engine).
4. **Cluster** validated points by date and to urban areas.
5. **Export** clean datasets of verified and clustered fires.

---

## 📦 Requirements

Before you begin:

- **NASA FIRMS API key**: Register for a [free API key](https://firms.modaps.eosdis.nasa.gov/api/map_key/). This is required to fetch fire detections from the NASA API. Copy [example.env](./example.env) to [.env](./.env) and fill in your acquired API key.
- **Google Earth Engine access**: We use Google Earth Engine to provide and analyse imagery from ESA's Sentinel-2 satellite. You need to register a (free for non-commercial) [Earth Engine project](https://developers.google.com/earth-engine/guides/access) on Google Cloud. To get the key follow these steps:
  1. Go to the [GCP Console](https://console.cloud.google.com) &rarr; Select/Create your project &rarr; APIs \& services &rarr; Google Earth Engine API &rarr; Credentials &rarr; Under Service accounts &rarr; Select/Create service account &rarr; Keys &rarr; Add key &rarr; json
  2. Put the downloaded json file in the [`./key`](./key/) directory.



- [uv](https://github.com/astral-sh/uv) (for installation, virtual environment and dependency management)

---

## 🚀 Quickstart

### 1. Clone this repository

```bash
git clone https://github.com/Centre-for-Information-Resilience/Fire_mapping
````

### 2. Add credentials
See Requirements section.

### 3. Configure project
For configuration of the project refer to the SQLMesh [config file](./sqlmesh/config.yaml). The following configuration options are currently available:

- `model_defaults`:
  - `start`: Set the start date of the project.
- `variables`
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
    - `clustering_max_date_gap`: Maximum gap between two consecutive FIRMS events used for clustering

    - # paths
    - `path_gadm`: Path to write gadm .gpkg files to
    - `path_geonames`: Path to write geonames .gpkg file
    - `path_output`: Path to write output to

    - `paths_areas`:
        - `include`: Path to inclusion areas .gpkg
        - `exclude`: Path to exclusion areas .gpkg 


### 3. Run the SQLMesh pipeline
For first time usage run:

```bash
uv run burnscar init
```
For subsequent runs you can use:
```bash
uv run burnscar run
```
These are two cli commands included for convenience, of course you can also just run the SQLMesh project directly. Make sure you're in [`sqlmesh/`](./sqlmesh/) and run:
```bash
uv run sqlmesh --help
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
src/burnscar/          # Python source (fetchers, validators, logic)
output/             # Final exported datasets
docs/               # Validation scripts & diagrams
```

---

## 🗺️ Data Sources

* **FIRMS** – NASA MODIS/VIIRS fire alerts: [nasa.gov](https://firms.modaps.eosdis.nasa.gov/map)
* **Copernicus Sentinel-2** – Post-burn surface reflectance: Accessed through Google Earth Engine, but can be explored using [Copernicus Browser](https://browser.dataspace.copernicus.eu)
* **GADM** – Global administrative boundaries: [gadm.org](https://gadm.org/)
* **GeoNames** - Open database of placenames: [geonames.org](https://www.geonames.org/)

---

## 🗃️ SQLMesh Model DAG
![SQLMesh Model DAG](./docs/dag.svg)

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

> ### Burnscar analyser
> Feed the CSV output from step 1 to Burnscar_Analyser_Latest_update 
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
> - Feed the filtered csv from step b to the Burnscar Analyse (step 2)
> - Follow the steps under 3 for the output, with the exception of step 3i (as this is the second time results come back as no image/too cloudy). Keep the remaining data for step 5.

> Manually analyse the remaining point on a map
> - Do the verification right away where possible
> - Discard locations that are low risk
> - Add everything else to the fire datasheet for the human verification

> Human verification in the final sheet, completing entries and reviewing before 

> ## Suggested priorities:

> Prio's 1a,b,c are related and should be ordered or merged if more convenient, but if I have to order them, then prio 1a would have the highest priority, and then b,c. This is in lesser extent also true for prio 2 and 3. Important note: It seems that the Burnscar Analyser stops checking for new Sentinel-2 imagery after there's no good imagery available for more than a number of days after the incident. I'll double check this with Micheal/Tarig, who have been working on both scripts. It would be good to know if it makes sense to extent this period for a bit, as I now still get more than 50% back with no image in the second run.

> - Prio 1a: Edit the Burnscar Analyser script so that there is a second output with all detections that come back as 'too cloud' or 'no image' in the same format as the input (saves step 4a-4c)
> - Prio 1b: Edit the Burnscar Analyser script so that it also works through the second output from prio 1a on request, while still having an output that distinguishes between these and the 'new' detections
> - Prio 1c: Merge the DataFetcher script with the Burnscar Analyser so that you only have to run one script instead of two, preferably by selecting a specific period you want to check (merges steps 1 and 2)
> - Prio 2a: Remove duplicate detections within the same boundary (if this is done before the process in GEE, this saves time/resources as well). The downside is that we then have to check for other detections within the same boundary on the same day, but I think this is more efficient. (saves step 3d)
> - Prio 2b: Merge (preferred way, e.g. by adding a column with number of days) and/or remove duplicate detections within the same boundary on subsequent days (saves time in the final verification process)
> - Prio 3: Edit the Burnscar Analyser script so that the first columns of the output are the same as column E-P (with column E on "In Progress" and F-J empty) in the 'Main' tab of the fire sheet (saves step 3f) 
> Anything else flagged by Lewis after reading through the steps and looking at scripts etc.

> Other potential improvements: 
> - Incorporate controlled list of village names 
> - Put damage assessment numbers back in
> - Use Planet as backup if S-2 fails (would be a major improvement, but also investment in development, quota)
