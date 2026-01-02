# World Bank Indicators Reference

This document provides a comprehensive list of commonly used World Bank indicator codes and their definitions.

## Table of Contents

- [Population Indicators](#population-indicators)
- [Economic Indicators (GDP)](#economic-indicators-gdp)
- [Emissions and Climate Indicators](#emissions-and-climate-indicators)
- [Energy Indicators](#energy-indicators)
- [Agriculture and Land Use](#agriculture-and-land-use)
- [Development Indicators](#development-indicators)

## Population Indicators

| Indicator Code | Definition |
|----------------|------------|
| SP.POP.TOTL | Population, total |
| SP.POP.GROW | Population growth (annual %) |
| SP.URB.TOTL | Urban population |
| SP.URB.GROW | Urban population growth (annual %) |
| SP.RUR.TOTL | Rural population |
| SP.RUR.TOTL.ZG | Rural population growth (annual %) |
| SP.DYN.CBRT.IN | Birth rate, crude (per 1,000 people) |
| SP.DYN.CDRT.IN | Death rate, crude (per 1,000 people) |
| SP.DYN.LE00.IN | Life expectancy at birth, total (years) |
| EN.POP.DNST | Population density (people per sq. km of land area) |

## Economic Indicators (GDP)

| Indicator Code | Definition |
|----------------|------------|
| NY.GDP.MKTP.CD | GDP (current US$) |
| NY.GDP.MKTP.KD | GDP (constant 2015 US$) |
| NY.GDP.MKTP.KD.ZG | GDP growth (annual %) |
| NY.GDP.PCAP.CD | GDP per capita (current US$) |
| NY.GDP.PCAP.KD | GDP per capita (constant 2015 US$) |
| NY.GDP.PCAP.KD.ZG | GDP per capita growth (annual %) |
| NY.GDP.PCAP.PP.CD | GDP per capita, PPP (current international $) |
| NY.GDP.PCAP.PP.KD | GDP per capita, PPP (constant 2017 international $) |
| NY.GNP.MKTP.CD | GNI (current US$) |
| NY.GNP.PCAP.CD | GNI per capita, Atlas method (current US$) |

## Emissions and Climate Indicators

### CO2 and Greenhouse Gas Emissions

| Indicator Code | Definition |
|----------------|------------|
| EN.ATM.CO2E.KT | CO2 emissions (kt) |
| EN.ATM.CO2E.PC | CO2 emissions (metric tons per capita) |
| EN.ATM.CO2E.GF.KT | CO2 emissions from gaseous fuel consumption (kt) |
| EN.ATM.CO2E.LF.KT | CO2 emissions from liquid fuel consumption (kt) |
| EN.ATM.CO2E.SF.KT | CO2 emissions from solid fuel consumption (kt) |
| EN.ATM.CO2E.KD.GD | CO2 emissions (kg per 2015 US$ of GDP) |
| EN.ATM.GHGT.KT.CE | Total greenhouse gas emissions (kt of CO2 equivalent) |
| EN.ATM.METH.KT.CE | Methane emissions (kt of CO2 equivalent) |
| EN.ATM.NOXE.KT.CE | Nitrous oxide emissions (thousand metric tons of CO2 equivalent) |
| EN.CO2.TRAN.ZS | CO2 emissions from transport (% of total fuel combustion) |
| EN.CO2.MANF.ZS | CO2 emissions from manufacturing industries and construction (% of total fuel combustion) |
| EN.CO2.ETOT.ZS | CO2 emissions from electricity and heat production (% of total fuel combustion) |

### Climate and Temperature

| Indicator Code | Definition |
|----------------|------------|
| AG.LND.PRCP.MM | Average precipitation in depth (mm per year) |
| EN.CLC.MDAT.ZS | Droughts, floods, extreme temperatures (% of population, average 1990-2009) |

## Energy Indicators

| Indicator Code | Definition |
|----------------|------------|
| EG.USE.PCAP.KG.OE | Energy use (kg of oil equivalent per capita) |
| EG.USE.COMM.FO.ZS | Fossil fuel energy consumption (% of total) |
| EG.FEC.RNEW.ZS | Renewable energy consumption (% of total final energy consumption) |
| EG.ELC.ACCS.ZS | Access to electricity (% of population) |
| EG.USE.ELEC.KH.PC | Electric power consumption (kWh per capita) |
| EG.ELC.RNEW.ZS | Electricity production from renewable sources (% of total) |
| EG.ELC.COAL.ZS | Electricity production from coal sources (% of total) |
| EG.ELC.NGAS.ZS | Electricity production from natural gas sources (% of total) |
| EG.ELC.NUCL.ZS | Electricity production from nuclear sources (% of total) |
| EG.ELC.PETR.ZS | Electricity production from oil sources (% of total) |

## Agriculture and Land Use

| Indicator Code | Definition |
|----------------|------------|
| AG.LND.AGRI.ZS | Agricultural land (% of land area) |
| AG.LND.FRST.ZS | Forest area (% of land area) |
| AG.LND.FRST.K2 | Forest area (sq. km) |
| AG.LND.CROP.ZS | Arable land (% of land area) |
| AG.YLD.CREL.KG | Cereal yield (kg per hectare) |
| AG.PRD.FOOD.XD | Food production index (2014-2016 = 100) |
| AG.CON.FERT.ZS | Fertilizer consumption (kilograms per hectare of arable land) |

## Development Indicators

| Indicator Code | Definition |
|----------------|------------|
| SI.POV.DDAY | Poverty headcount ratio at $2.15 a day (2017 PPP) (% of population) |
| SI.POV.GINI | GINI index (World Bank estimate) |
| SE.PRM.ENRR | School enrollment, primary (% gross) |
| SE.SEC.ENRR | School enrollment, secondary (% gross) |
| SE.TER.ENRR | School enrollment, tertiary (% gross) |
| SH.DYN.MORT | Mortality rate, under-5 (per 1,000 live births) |
| SH.DYN.NMRT | Mortality rate, neonatal (per 1,000 live births) |
| SH.STA.MMRT | Maternal mortality ratio (modeled estimate, per 100,000 live births) |

## Additional Resources

### Finding More Indicators

To search for additional indicators, you can:

1. Visit the [World Bank Data Catalog](https://datacatalog.worldbank.org/)
2. Use the API to list all indicators: `https://api.worldbank.org/v2/indicator?format=json`
3. Browse indicators by topic: `https://api.worldbank.org/v2/topic?format=json`

### Indicator Metadata

To get detailed metadata about any indicator, use:
```
https://api.worldbank.org/v2/indicator/{indicator_code}?format=json
```

For example:
```
https://api.worldbank.org/v2/indicator/SP.POP.TOTL?format=json
```

## Notes

- Indicator availability varies by country and year
- Some indicators are updated annually, while others may have different update frequencies
- Historical data availability differs across indicators
- Use the `per_page` parameter in API calls to retrieve more results (default is 50, maximum is 32,500)
