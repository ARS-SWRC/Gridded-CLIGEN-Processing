import ee
import time

#ee.Authenticate()
ee.Initialize()


product_name = 'USDOS/LSIB_SIMPLE/2017'

borders_shp = ee.FeatureCollection( 'USDOS/LSIB_SIMPLE/2017' )
kgz_shp = borders_shp.filterMetadata( 'country_na', 'equals', 'Kyrgyzstan' )
tjk_shp = borders_shp.filterMetadata( 'country_na', 'equals', 'Tajikistan' )
merge_shp = kgz_shp.merge( tjk_shp )
union_shp = merge_shp.union()

start_period = ee.Date( '2018-07-01' )
end_period = ee.Date( '2018-07-02' )

era_ic = ee.ImageCollection( 'ECMWF/ERA5/DAILY' )                              \
    .filter( ee.Filter.date( start_period, end_period ) )                      \
    .select( ['total_precipitation'] )

# era_ic = ee.ImageCollection( 'NASA/GPM_L3/IMERG_V06' )                         \
#     .filter( ee.Filter.date( start_period, end_period ) )                      \
#     .select( ['precipitationCal'] )

# era_ic = ee.ImageCollection( 'NASA/GLDAS/V021/NOAH/G025/T3H' )                 \
#     .filter( ee.Filter.date( start_period, end_period ) )                      \
#     .select( ['SWdown_f_tavg'] )

def clip( im ):
    return im.clip( union_shp )

eraClip_ic = era_ic.map( clip )

listOfImages = eraClip_ic.toList( eraClip_ic.size() )
sample_im = ee.Image( listOfImages.get( 0 ) )

vectors_fc = sample_im.sample( geometries=True, region=union_shp )

listOfFeats = vectors_fc.toList( vectors_fc.size() )

def set_properties( ft ):
    ft = ee.Feature( ft )
    coords = ft.geometry().coordinates()
    x = ee.Number( coords.get( 0 ) ).format( '%.3f'  )
    y = ee.Number( coords.get( 1 ) ).format( '%.3f'  )
    stationID = x.cat( ' ' ).cat( y )
    ft = ft.set( 'stationID', stationID )
    return ft
    
vectors_fc = ee.FeatureCollection( vectors_fc.map( set_properties ) )

task = ee.batch.Export.table.toAsset( collection=vectors_fc,
                                      assetId='users/andrewfullhart/KGZ_TJK_ERA_Grid' )

task.start()

print('Running export to Cloud Storage...')

while task.active():
    time.sleep( 1 )

if task.status()['state'] != 'COMPLETED':
    print('Export error.')
else:
    print('Export completed.')

