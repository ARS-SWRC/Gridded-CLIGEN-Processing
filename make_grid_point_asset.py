#GO TO LINE 49: set path to new asset
import ee
import time

#ee.Authenticate()
ee.Initialize()


product_name = 'TIGER/2016/Counties'

borders_shp = ee.FeatureCollection( product_name )
pima_shp = borders_shp.filterMetadata( 'STATEFP', 'equals', '04' ).filterMetadata( 'NAME', 'equals', 'Pima' )
santacruz_shp = borders_shp.filterMetadata( 'STATEFP', 'equals', '04' ).filterMetadata( 'NAME', 'equals', 'Santa Cruz' )
merge_shp = pima_shp.merge( santacruz_shp )
union_shp = merge_shp.union()

start_period = ee.Date( '2018-07-01' )
end_period = ee.Date( '2018-07-02' )

era_ic = ee.ImageCollection( 'ECMWF/ERA5/DAILY' )                              \
    .filter( ee.Filter.date( start_period, end_period ) )                      \
    .select( ['total_precipitation'] )

def clip( im ):
    return im.clip( union_shp )

eraClip_ic = era_ic.map( clip )

listOfImages = eraClip_ic.toList( eraClip_ic.size() )
sample_im = ee.Image( listOfImages.get( 0 ) )

vectors_fc = sample_im.sample( geometries=True, region=union_shp )
vectors_fc = vectors_fc.select( ['system:index', 'stationID'] )

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
                                      assetId='users/andrewfullhart/PimaSantaCruz_ERA_Grid' )

task.start()

print('Running export to Cloud Storage...')

while task.active():
    time.sleep( 1 )

if task.status()['state'] != 'COMPLETED':
    print('Export error.')
else:
    print('Export completed.')


