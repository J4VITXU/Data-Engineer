# Quick Reference Guide

This guide provides practical examples for querying data from the World Bank APIs.

## Table of Contents

- [Basic API Structure](#basic-api-structure)
- [Querying Countries](#querying-countries)
- [Querying Regions](#querying-regions)
- [Querying Indicators](#querying-indicators)
- [Filtering by Date](#filtering-by-date)
- [Population Data](#population-data)
- [Economic Growth Data](#economic-growth-data)
- [GDP Data](#gdp-data)
- [Emissions Data](#emissions-data)
- [Climate Data API](#climate-data-api)
- [Advanced Query Parameters](#advanced-query-parameters)

## Basic API Structure

The World Bank Indicators API follows this pattern:
```
https://api.worldbank.org/v2/{entity}/{query}?format={format}&parameter=value
```

Common formats:
- `json` - JSON format (recommended)
- `xml` - XML format
- `jsonstat` - JSON-stat format

## Querying Countries

### Get all countries
```
https://api.worldbank.org/v2/country?format=json
```

### Get specific country information
```
https://api.worldbank.org/v2/country/USA?format=json
https://api.worldbank.org/v2/country/BRA?format=json
https://api.worldbank.org/v2/country/CHN?format=json
```

### Get multiple countries
```
https://api.worldbank.org/v2/country/USA;CHN;IND?format=json
```

### Common country codes
- USA - United States
- CHN - China
- IND - India
- BRA - Brazil
- GBR - United Kingdom
- DEU - Germany
- JPN - Japan
- CAN - Canada
- AUS - Australia
- MEX - Mexico

## Querying Regions

### Get all regions
```
https://api.worldbank.org/v2/region?format=json
```

### Common region codes
- EAS - East Asia & Pacific
- ECS - Europe & Central Asia
- LCN - Latin America & Caribbean
- MEA - Middle East & North Africa
- NAC - North America
- SAS - South Asia
- SSF - Sub-Saharan Africa
- WLD - World

### Get region information
```
https://api.worldbank.org/v2/region/EAS?format=json
```

## Querying Indicators

### Get all available indicators
```
https://api.worldbank.org/v2/indicator?format=json&per_page=100
```

### Get indicator metadata
```
https://api.worldbank.org/v2/indicator/SP.POP.TOTL?format=json
```

### Get indicator data for a country
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json
```

### Get indicator data for multiple countries
```
https://api.worldbank.org/v2/country/USA;CHN;IND/indicator/SP.POP.TOTL?format=json
```

### Get indicator data for a region
```
https://api.worldbank.org/v2/country/EAS/indicator/SP.POP.TOTL?format=json
```

## Filtering by Date

### Get data for a specific year
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?date=2020&format=json
```

### Get data for a date range
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?date=2010:2020&format=json
```

### Get data for multiple specific years
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?date=2010;2015;2020&format=json
```

### Get most recent values
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?date=2020:2023&format=json&mrnev=1
```
*Note: `mrnev=1` returns Most Recent Non-Empty Value*

## Population Data

### Total population
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json
```

### Population growth rate
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.GROW?date=2010:2023&format=json
```

### Urban population
```
https://api.worldbank.org/v2/country/USA/indicator/SP.URB.TOTL?format=json
```

### Population density
```
https://api.worldbank.org/v2/country/USA/indicator/EN.POP.DNST?format=json
```

### Compare population across countries
```
https://api.worldbank.org/v2/country/USA;CHN;IND;BRA/indicator/SP.POP.TOTL?date=2022&format=json
```

## Economic Growth Data

### GDP growth rate
```
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.KD.ZG?date=2010:2023&format=json
```

### GDP per capita growth
```
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.PCAP.KD.ZG?date=2010:2023&format=json
```

### Compare growth rates across regions
```
https://api.worldbank.org/v2/country/EAS;ECS;LCN;SSF/indicator/NY.GDP.MKTP.KD.ZG?date=2022&format=json
```

## GDP Data

### GDP (current US$)
```
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?date=2022&format=json
```

### GDP (constant 2015 US$)
```
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.KD?date=2022&format=json
```

### GDP per capita (current US$)
```
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.PCAP.CD?date=2022&format=json
```

### GDP per capita, PPP
```
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.PCAP.PP.CD?date=2022&format=json
```

### Compare GDP across major economies
```
https://api.worldbank.org/v2/country/USA;CHN;JPN;DEU;GBR/indicator/NY.GDP.MKTP.CD?date=2022&format=json
```

## Emissions Data

### Total CO2 emissions
```
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.CO2E.KT?date=2010:2020&format=json
```

### CO2 emissions per capita
```
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.CO2E.PC?date=2010:2020&format=json
```

### CO2 emissions intensity (per GDP)
```
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.CO2E.KD.GD?date=2010:2020&format=json
```

### Total greenhouse gas emissions
```
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.GHGT.KT.CE?date=2010:2020&format=json
```

### Compare emissions across countries
```
https://api.worldbank.org/v2/country/USA;CHN;IND;DEU;JPN/indicator/EN.ATM.CO2E.KT?date=2020&format=json
```

### Methane emissions
```
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.METH.KT.CE?date=2010:2020&format=json
```

### CO2 from transport
```
https://api.worldbank.org/v2/country/USA/indicator/EN.CO2.TRAN.ZS?date=2020&format=json
```

## Climate Data API

The Climate Data API uses a different structure:
```
https://climateknowledgeportal.worldbank.org/api/data/{type}/{statistic}/{variable}/{country}/{gcm}
```

### Historical Climate Data

#### Get historical temperature data
```
https://climateknowledgeportal.worldbank.org/api/data/get/historical/tas/usa
```

#### Get historical precipitation data
```
https://climateknowledgeportal.worldbank.org/api/data/get/historical/pr/usa
```

### Climate Projections

#### Get temperature projections
```
https://climateknowledgeportal.worldbank.org/api/data/get/projection/tas/usa
```

#### Get precipitation projections
```
https://climateknowledgeportal.worldbank.org/api/data/get/projection/pr/usa
```

### Climate Variables
- `tas` - Temperature (mean)
- `tasmin` - Minimum temperature
- `tasmax` - Maximum temperature
- `pr` - Precipitation

## Advanced Query Parameters

### Pagination
```
https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=100&page=1
```

### Most Recent Value Only
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json&mrnev=1
```

### Get most recent N values
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json&mrnev=5
```

### Frequency (monthly, quarterly, yearly)
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json&frequency=Y
```
*Options: M (monthly), Q (quarterly), Y (yearly)*

### Gap Fill
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json&gapfill=Y
```
*Fills in missing values using interpolation*

### Source Filter
```
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?format=json&source=2
```
*Filter by specific data source ID*

## Combined Examples

### Population and GDP for multiple countries over time
```
https://api.worldbank.org/v2/country/USA;CHN;IND/indicator/SP.POP.TOTL?date=2010:2023&format=json
https://api.worldbank.org/v2/country/USA;CHN;IND/indicator/NY.GDP.MKTP.CD?date=2010:2023&format=json
```

### Emissions and economic data comparison
```
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.CO2E.KT?date=2000:2020&format=json
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.KD.ZG?date=2000:2020&format=json
```

### Regional aggregates
```
https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL?date=2022&format=json
https://api.worldbank.org/v2/country/WLD/indicator/EN.ATM.CO2E.KT?date=2020&format=json
```

### Multiple indicators for one country
```
# Population
https://api.worldbank.org/v2/country/USA/indicator/SP.POP.TOTL?date=2022&format=json

# GDP
https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?date=2022&format=json

# CO2 Emissions
https://api.worldbank.org/v2/country/USA/indicator/EN.ATM.CO2E.KT?date=2020&format=json

# Energy Use
https://api.worldbank.org/v2/country/USA/indicator/EG.USE.PCAP.KG.OE?date=2020&format=json
```

## Tips and Best Practices

1. **Use pagination**: Set `per_page` parameter to get more results (max 32,500)
2. **Check data availability**: Not all indicators are available for all countries and years
3. **Use date ranges wisely**: Narrow date ranges return faster
4. **Cache responses**: API responses can be cached to reduce load
5. **Handle missing data**: Many indicators have gaps in historical data
6. **Use MRNEV**: When you just need the latest available value, use `mrnev=1`
7. **Combine with other tools**: Parse JSON responses with tools like `jq` or programming languages
8. **Rate limiting**: Be respectful of API rate limits when making bulk requests

## Example Response Format

A typical JSON response looks like:
```json
[
  {
    "page": 1,
    "pages": 1,
    "per_page": 50,
    "total": 63
  },
  [
    {
      "indicator": {
        "id": "SP.POP.TOTL",
        "value": "Population, total"
      },
      "country": {
        "id": "US",
        "value": "United States"
      },
      "countryiso3code": "USA",
      "date": "2022",
      "value": 333287557,
      "unit": "",
      "obs_status": "",
      "decimal": 0
    }
  ]
]
```

## Additional Resources

- [World Bank Open Data](https://data.worldbank.org/)
- [API Documentation](https://datahelpdesk.worldbank.org/knowledgebase/topics/125589)
- [Climate Knowledge Portal](https://climateknowledgeportal.worldbank.org/)
- [Indicators Reference](indicators.MD)
