# Fire Detection and Mapping 

### This is a work in progress proejct that is aimed at refactoring a number of different components. 
### Minimum objective is to make this easier to run and to better manage dependencies 
### Ideal outcome is to have this deployable. 

## How to run
### Data Fetcher
Run DataFetcher (gets FIRMS data points from FIRMS API. You first need to insert a map key in line 9)
The API allows download in batches of 10 days, the script therefore uses 9 days increments. You need to put the start date in line 22, (I always put the current or last date of the period I'm using to triage – see flow chart – in a note in that line for the next time I'm running this script
To prevent FIRMS rate limiting or blocking the map key, the scripts sleeps in between increments, so it takes a while.
For my own convenience, I've added a line print(f"Currently checking for date {current_date}") in the While loop at the end, so that I can see where the script currently is. Note that "current date" means a period of 10 days up to that date. (If this line does not already exists in the shared script) 
Note that the script continues even after reaching the current day. I stop running as soon the script returns no new entries/current date is 10 days over the last date I want to include in the period.

### Arson analyser
Feed the CSV output from step 1 to Arson_Analyser_Latest_update 
There's 3 things you need to do before you can run this script: 
(a) Create a Google Earth Engine project name in line 70, you can create a project here, 
(b) Edit the base directory in line 16
(c) Have the data.csv output from the first step ready in the correct folder, see line 17. If you want to know a bit about what the script and GEE do, you can read a summary of the methodology here
Insert the CSV output from step 2 in Google Sheets as a new tab into the fire sheet for analysis and triage:
- Rename tab, e.g. 'Jan_16' (Jan_16 from here on)
- Sort column 'burn_scar_detected' from Z to A
- Check if there are any cities with the same unique event number in 'unique_event_no'. If yes -> change unique event no so that these entries won't get deleted in next step
- Remove duplicate entries (entries that were created because of multiple detections at same location, columns to analyze: D - Name_3 / E Urban area /I Unique event number)
- Check the entries with FALSE in burn_scar_detected for entries with a 'high risk' location (area with recent combat activity/reported conflict around the date/location)
- Put the name of Jan_16 in cell S2 of the 'Conversion' tab (this will reformat the entries so they can be easily copy-pasted into the main tab)
- Copy-paste the entries from the 'Conversion' tab that have 'TRUE', and optionally the entries identified in step 3e, in the 'burn_scar_detected' column to the 'Main' tab of the fire sheet
- Discard the entries in Jan_16 that were copy-pasted to the main tab
- Keep the remaining 'too cloudy' and 'no image' entries in the tab for the next run

[Next run, minimal 5 days later] Second run for the 'too cloudy' and 'no image' entries (this process is shown by the red dotted lines in the flow chart)
- Run the DataFetcher (see step 1) for the period that covers all remaining entries from 3i
- Filter the DataFetcher output so that only detections remain that are also in the list with remaining entries from 3i (I do this in Google Sheets, I can explain this if you want me to)
- Feed the filtered csv from step b to the Arson Analyse (step 2)
- Follow the steps under 3 for the output, with the exception of step 3i (as this is the second time results come back as no image/too cloudy). Keep the remaining data for step 5.

Manually analyse the remaining point on a map
- Do the verification right away where possible
- Discard locations that are low risk
- Add everything else to the fire datasheet for the human verification

Human verification in the final sheet, completing entries and reviewing before 

## Suggested priorities:

Prio's 1a,b,c are related and should be ordered or merged if more convenient, but if I have to order them, then prio 1a would have the highest priority, and then b,c. This is in lesser extent also true for prio 2 and 3. Important note: It seems that the Arson Analyser stops checking for new Sentinel-2 imagery after there's no good imagery available for more than a number of days after the incident. I'll double check this with Micheal/***REMOVED***, who have been working on both scripts. It would be good to know if it makes sense to extent this period for a bit, as I now still get more than 50% back with no image in the second run.

Prio 1a: Edit the Arson Analyser script so that there is a second output with all detections that come back as 'too cloud' or 'no image' in the same format as the input (saves step 4a-4c)
Prio 1b: Edit the Arson Analyser script so that it also works through the second output from prio 1a on request, while still having an output that distinguishes between these and the 'new' detections
Prio 1c: Merge the DataFetcher script with the Arson Analyser so that you only have to run one script instead of two, preferably by selecting a specific period you want to check (merges steps 1 and 2)
Prio 2a: Remove duplicate detections within the same boundary (if this is done before the process in GEE, this saves time/resources as well). The downside is that we then have to check for other detections within the same boundary on the same day, but I think this is more efficient. (saves step 3d)
Prio 2b: Merge (preferred way, e.g. by adding a column with number of days) and/or remove duplicate detections within the same boundary on subsequent days (saves time in the final verification process)
Prio 3: Edit the Arson Analyser script so that the first columns of the output are the same as column E-P (with column E on "In Progress" and F-J empty) in the 'Main' tab of the fire sheet (saves step 3f) 
Anything else flagged by Lewis after reading through the steps and looking at scripts etc.

Other potential improvements: 
Incorporate controlled list of village names 
Put damage assessment numbers back in
Use Planet as backup if S-2 fails (would be a major improvement, but also investment in development, quota)
