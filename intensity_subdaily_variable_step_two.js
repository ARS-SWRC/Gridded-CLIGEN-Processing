/*

Script: intensity_subdaily_variable_step_two.js
Goal: To extract GPM climatology monthly mean values at each specific location (stations)
Inputs: 
  - Pre-computed means using script `GPM_monthly_means_generator.js`
  - Point features (stations) 
Outputs:
  - A CSV file with the mean monthly values for each station |stationdID|prcp_01|prcp_02|prcp_03|...
  
*/


// Load stations data
var asset = ee.FeatureCollection( 'users/andrewfullhart/Northern_ERA_Grid' )

// Load the pre-computed means 
var jan = ee.Image("users/gponce/usda_ars/assets/01_jan_mean_2001_2021_GPM"),
    feb = ee.Image("users/gponce/usda_ars/assets/02_feb_mean_2001_2021_GPM"),
    mar = ee.Image("users/gponce/usda_ars/assets/03_mar_mean_2001_2021_GPM"),
    apr = ee.Image("users/gponce/usda_ars/assets/04_apr_mean_2001_2021_GPM"),
    may = ee.Image("users/gponce/usda_ars/assets/05_may_mean_2001_2021_GPM"),
    jun = ee.Image("users/gponce/usda_ars/assets/06_jun_mean_2001_2021_GPM"),
    jul = ee.Image("users/gponce/usda_ars/assets/07_jul_mean_2001_2021_GPM"),
    aug = ee.Image("users/gponce/usda_ars/assets/08_aug_mean_2001_2021_GPM"),
    sep = ee.Image("users/gponce/usda_ars/assets/09_sep_mean_2001_2021_GPM"),
    oct = ee.Image("users/gponce/usda_ars/assets/10_oct_mean_2001_2021_GPM"),
    nov = ee.Image("users/gponce/usda_ars/assets/11_nov_mean_2001_2021_GPM"),
    dec = ee.Image("users/gponce/usda_ars/assets/12_dec_mean_2001_2021_GPM");

// Get projection information
var proj=jan.projection()

// Create a multi-band image with the climatology of monthly precipitation 
var img = jan.addBands(feb).addBands(mar)
            .addBands(apr).addBands(may)
            .addBands(jun).addBands(jul)
            .addBands(aug).addBands(sep)
            .addBands(oct).addBands(nov)
            .addBands(dec)

// Reduce image at each StationID
var results = img.reduceRegions({collection:asset, 
                                 reducer:ee.Reducer.mean(),
                                 scale: proj.nominalScale(), 
                                 tileScale:4})

// Export the results
Export.table.toDrive({collection:results, 
                      description:'export_all', 
                      fileNamePrefix:'export_all', 
                      selectors: ['stationID','prcp_01','prcp_02','prcp_03','prcp_04','prcp_05','prcp_06','prcp_07','prcp_08','prcp_09','prcp_10','prcp_11','prcp_12'],
                      fileFormat:'CSV'})

//print(results.limit(10))                                 
