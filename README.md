# Gridded CLIGEN Processing
## _GEE code that calculates CLIGEN input parameters from global climate products_

The scripts in this repository [ARS-SWRC/Gridded-CLIGEN-Processing]
are used to calculate CLIGEN input parameters directly from global climate
datasets. Bias adjustments for the resulting parameter values are not currently
part of this code. The scripts only require a GEE asset representing point geometries.
To create this asset, [make_grid_point_asset.py] may be used to get grid points at the resolution
of selected climate products for clipped regions. 

# Goals
- Create Google Earth Engine code to calculate CLIGEN input parameters.
- Test possibilities for optimizing the code to reduce run-time.
- Run the code for large regions.

# GEE Point Geometry Assets
| Description | Path | n | Resolution |
| ------ | ------ | ------ | ------ |
| Northern Latitude Band Grid | ("users/andrewfullhart/Northern_ERA_Grid") | 114952 | 0.25 arc deg |
| Southern Latitude Band Grid | ("users/andrewfullhart/Southern_GPM_Grid") | 37105 | 0.25 arc deg |

# Monthly CLIGEN Input Parameters

| Parameter	| Label	| Script | Climate Product | Band |
| ------ | ------ | ------ | ------ | ------ |
| Mean of daily precip. for "wet" days in the month | MEAN P | [precip_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | total_precipitation |
| Standard deviation of daily precip. for "wet" days in the month | S DEV P | [precip_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | total_precipitation |
| Skewness of daily precip. for "wet" days in the month | SKEW P | [precip_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | total_precipitation | 
| Transition probability of a wet day given a wet day | P(W/W) | [precip_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | total_precipitation |
| Transition probability of a wet day given a dry day | P(W/D) | [precip_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | total_precipitation |
| Maximum 30-min intensity in month averaged by years | MX.5P | [intensity_subdaily_variable.py] | [GPM][linkB] ("NASA/GPM_L3/IMERG_V06") | precipitationCal |
| Time-to-peak-intensity cumulative probability dist. | TimePk | - | - | - |
| Mean of maximum daily temperature | TMAX AV | [temp_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | maximum_2m_air_temperature |
| Mean of minimum daily temperature | TMIN AV | [temp_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | minimum_2m_air_temperature |
| Standard deviation of maximum daily temperature | SD TMAX | [temp_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | maximum_2m_air_temperature |
| Standard deviation of minimum daily temperature | SD TMIN | [temp_daily_variables.py] | [ERA5][linkA] ("ECMWF/ERA5/DAILY") | minimum_2m_air_temperature |
| Mean of Daily Incoming Solar Radiation | SOL.RAD | [solrad_daily_variables.py] | [GLDAS][linkC] ("NASA/GLDAS/V021/NOAH/G025/T3H") | SWdown_f_tavg |
| Standard Deviation of Daily Incoming Solar Radiation | SD SOL | [solrad_daily_variables.py] | [GLDAS][linkC] ("NASA/GLDAS/V021/NOAH/G025/T3H") | SWdown_f_tavg |
| Various Wind Parameters | - | - | - | - |



[ARS-SWRC/Gridded-CLIGEN-Processing]: <https://github.com/ARS-SWRC/Gridded-CLIGEN-Processing>
[make_grid_point_asset.py]: <https://github.com/ARS-SWRC/Gridded-CLIGEN-Processing/blob/main/make_grid_point_asset.py>
[precip_daily_variables.py]: <https://github.com/ARS-SWRC/Gridded-CLIGEN-Processing/blob/main/precip_daily_variables.py>
[intensity_subdaily_variable.py]:<https://github.com/ARS-SWRC/Gridded-CLIGEN-Processing/blob/main/intensity_subdaily_variable.py>
[temp_daily_variables.py]:<https://github.com/ARS-SWRC/Gridded-CLIGEN-Processing/blob/main/temp_daily_variables.py>
[solrad_daily_variables.py]:<https://github.com/ARS-SWRC/Gridded-CLIGEN-Processing/blob/main/temp_daily_variables.py>

[linkA]: <https://developers.google.com/earth-engine/datasets/catalog/ECMWF_ERA5_DAILY#description>
[linkB]: <https://developers.google.com/earth-engine/datasets/catalog/NASA_GPM_L3_IMERG_V06#description>
[linkC]: <https://developers.google.com/earth-engine/datasets/catalog/NASA_GLDAS_V021_NOAH_G025_T3H#description>

