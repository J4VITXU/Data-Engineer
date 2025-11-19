a. What kind of data is?
i. The data belongs to Divvy Bike Sharing (Chicago’s bike-share system).
It contains trip-level data, where each record represents an individual bike ride — including start time, end time, station, duration, user type, etc.

b. Can you think of analyses that can be made with this?
i. Looking at the data fields (trip start time, end time, stations, user type, etc.), one can analyze:
Usage patterns (by hour, day, or month)
Station popularity (most used start/end stations)
Trip duration statistics
Customer behavior (member vs casual users)
Seasonal or yearly trends
Bike availability and balancing needs across stations

c. Is the data normalized or denormalized?
It’s denormalized.

d. It’s needed any processing before we use it?
Yes, the script only downloads and extracts the .zip files.
Before analysis, it’s necessary to:
Load the CSVs into pandas or another tool.
Merge datasets across quarters/years.
Convert date/time columns to datetime objects.
Handle missing values and data type inconsistencies.
Fix the typo in one URL

e. Are there null values? In which fields? Measure how many and guess a reason.
Typically, yes — Divvy trip data often contains nulls in:
Station names or IDs (when the ride starts/ends outside the official system or data is missing)
Member type or gender fields (if not collected for casual users)