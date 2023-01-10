/*

Script: intensity_subdaily_variable_step_one.js.js
Goal: To generate climatology of monthly means from GPM dataset
Inputs: 
  - GPM L3 IMERG_V06
  - Point features (stations) 
Outputs:
  - Single image layer as asset for each month of the year
  
*/

// Load data points 
var asset = ee.FeatureCollection( 'users/andrewfullhart/Northern_ERA_Grid')

// Get boundary for filtering image collection.  In some cases filtering by bounds can help to speed up the processing
var bounds = asset.geometry().bounds()

// Load climate data GPM
var GPM = ee.ImageCollection("NASA/GPM_L3/IMERG_V06").select('precipitationCal').filterBounds(bounds)

// Get projection information
var proj=GPM.first().projection()

// Define start/end date variables
var startyear = 2001; 
var endyear = 2020

var startdate = ee.Date.fromYMD(startyear, 1, 1);

var enddate = ee.Date.fromYMD(endyear + 1, 1, 1);

var years = ee.List.sequence(startyear, endyear);

var months = ee.List.sequence(1, 12);

              
// Get Max monthly, filterDate should be used first always before any other filtering method.
var ic_gpm_max_monthly =  ee.ImageCollection.fromImages(
 years.map(function (y) {
    return months.map(function(m) {
      var v_start = ee.Date.fromYMD(y, m, 1);
      var v_end = v_start.advance(1, 'month')
      var gpm_max_monthly = GPM
                    .filterDate(v_start, v_end)
                    // .filter(ee.Filter.calendarRange(y, y, 'year'))
                    // .filter(ee.Filter.calendarRange(m, m, 'month'))
                    .max()
      return gpm_max_monthly
                 .set('year', y)
                 .set('month', m)
                 .set('system:time_start', ee.Date.fromYMD(y, m, 1).millis());
    })
  }).flatten()
)

// Get the 12 months aggregation image list
var results = 
  months.map(function (m) {
    var GPM_month = ic_gpm_max_monthly
                                  .filterDate(startdate, enddate)
                                  .filter(ee.Filter.calendarRange(m, m, 'month'))
                                  .mean()
                                  .rename(ee.String('precipitation_').cat(ee.Number(m).format('%02d')))
      return ee.Image(GPM_month)
  })


// Export individual images. Note: This is the fastest way to export the intermediate mean images to be used in
// script ~/AFULLHART/all_stations_extraction.js

// Set the path for exporting the asset
var v_dir = 'users/gponce/usda_ars/assets/'

// Export results for each month
for (var i = 0; i < 12; i++) {
  var m = i + 1
  Export.image.toAsset({image:ee.Image(results.get(i))
                                    .rename(['prcp_'+ m]), 
                        description: 'prcp_' + m + '_mean_2001_2021_GPM', 
                        assetId: v_dir + 'prcp_' + m + '_mean_2001_2021_GPM',
                        region:bounds, 
                        scale:proj.nominalScale().getInfo(), 
                        crs:proj.crs().getInfo(),
                        maxPixels:1e13})
}

