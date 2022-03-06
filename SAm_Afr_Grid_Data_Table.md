|  |  | South America | - | - | > |  | Africa | - | - | > |  |  |  |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CLIGEN Monthly Parameters | Unit | Min | Max | Avg | Median |  | Min | Max | Avg | Median |  | Dataset Input | Statistic Definition |
| MEAN P | inches | 0.09 | 1.23 | 0.49 | 0.50 |  | 0.07 | 1.59 | 0.34 | 0.33 |  | ERA5 | Mean of single-event accumulation[^3] |
| S DEV P | inches | 0.07 | 1.60 | 0.63 | 0.67 |  | 0.01 | 1.49 | 0.38 | 0.37 |  | ERA5 | Standard deviation of single-event accumulation[^3] |
| SKEW P | - | 0.49 | 5.10 | 2.65 | 2.72 |  | 0.04 | 5.02 | 1.68 | 1.99 |  | ERA5 | Skewness of single-event accumulation[^3] |
| P(W/W) | - | 0.00 | 1.00 | 0.47 | 0.47 |  | 0.00 | 1.00 | 0.29 | 0.28 |  | ERA5 | Conditional transition probability of wet day following wet day |
| P(W/D) | - | 0.00 | 1.00 | 0.20 | 0.18 |  | 0.00 | 1.00 | 0.10 | 0.03 |  | ERA5 | Conditional transition probability of wet day following dry day |
| TMAX AV | Fahrenheit | 10.42 | 101.63 | 79.95 | 85.15 |  | 40.83 | 117.00 | 87.01 | 86.72 |  | ERA5 | Mean daily maximum temperature |
| TMIN AV | Fahrenheit | -2.60 | 81.26 | 63.19 | 69.53 |  | 22.21 | 90.77 | 66.02 | 68.04 |  | ERA5 | Mean daily minimum temperature |
| SD TMAX | Fahrenheit | 0.47 | 11.85 | 4.57 | 3.85 |  | 0.53 | 12.05 | 4.49 | 4.00 |  | ERA5 | Standard deviation of daily maximum temperature |
| SD TMIN | Fahrenheit | 0.51 | 10.83 | 3.24 | 2.23 |  | 0.56 | 8.82 | 3.49 | 3.32 |  | ERA5 | Standard deviation of daily minimum temperature |
| SOL.RAD | Langley | 0.00[^2] | 819.00 | 433.66 | 442.00 |  | 0.00[^2] | 777.00 | 495.65 | 499.00 |  | GLDAS | Mean daily incoming solar radiation |
| SD SOL | Langley | 0.00[^2] | 209.10 | 100.20 | 102.60 |  | 0.00[^2] | 324.00 | 78.63 | 78.10 |  | GLDAS | Standard deviation of incoming solar radiation |
| MX .5 P | inches/hr | 0.04 | 1.75 | 0.93 | 1.09 |  | 0.05 | 1.84 | 0.60 | 0.33 |  | GPM-IMERG | Mean maximum 30-minute precipitation intensity |
| DEW PT | Fahrenheit | -19.53 | 78.62 | 60.38 | 66.32 |  | 9.81 | 80.91 | 51.43 | 52.15 |  | ERA5 | Mean dewpoint temperature |
| Time Pk[^1] | - | 0.07 | 1.00 | 0.65 | 0.69 |  | 0.07 | 1.00 | 0.64 | 0.69 |  | Representative Ground Data | Cumulative probability of normalized time-to-peak intensity |
| Wind Parameters | Various | - | - | - | - |  | - | - | - | - | - | Representative Ground Data  |  |

[^1]: Time Pk is the only parameter that does not represent a monthly statistic.
[^2]: Zero values due to gaps in GLDAS coverage for 0.5% and 0.7% of grid points in South America and Africa, respectively.
[^3]: CLIGEN assumes a maximum of one precipitation event per day.
