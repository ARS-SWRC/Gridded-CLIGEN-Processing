
import ee
import datetime as dt
import time
import math

now = dt.datetime.now()

#ee.Authenticate()
ee.Initialize()

product_name_ = 'NASA/GPM_L3/IMERG_V06' 
band_labels_ = ['precipitationCal']
asset = ee.FeatureCollection( 'users/andrewfullhart/PimaSantaCruz_ERA_Grid' )

start = '2001-01-01'
end = '2021-01-01'

stations_per_batch = 10

stations = ee.FeatureCollection( asset )

'slice stations in case of restart'
#station_feats = stations.toList( 10000000 )
#stations = ee.FeatureCollection( station_feats.slice( 720 ) )

stationIDs = ee.List( stations.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )

station_groups = []
tmp = []
ct = 0
batch_ct = int(math.ceil( stationIDs.size().getInfo() / stations_per_batch ))
for i in range(stationIDs.size().getInfo()):
    tmp.append( stationIDs.get( i ) )
    ct += 1
    if ct == stations_per_batch:
        station_groups.append( tmp )
        tmp = []
        ct = 0
    else:
        pass
        
station_groups.append( tmp )            
station_groups_ = ee.List( station_groups )

start_date = ee.Date( start )
end_date = ee.Date( end )
yr_ct = end_date.difference( start_date, 'year' ).round().getInfo()
start_yr = int(start.split( '-' )[0])
yr_mo_list = []
for yr in range(start_yr, start_yr+yr_ct):
    for mo in range(1, 13):
        yr_mo_list.append( [yr, mo] )  
yr_mo_list = ee.List( yr_mo_list )
mo_index_seq_ = ee.List.sequence( 0, 11 )
yr_index_seq_ = ee.List.sequence( 0, yr_ct-1 )


def get_column_from_nested_lists( list_obj ):
    data_list = ee.List( list_obj )
    data = ee.Number( data_list.get( 4 ) )
    return data

for batch_i in range(batch_ct):
    stationID_batch_list = ee.List( station_groups_.get( batch_i ) )
    batch_filter = ee.Filter.inList( 'stationID', stationID_batch_list )
    stations_ = stations.filter( batch_filter )
    stations_list_ = stations_.toList( 10000000 )
    station_ids = ee.List( stations_.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )
    station_ids_strs_ = ee.List( [str(elem) for elem in station_ids.getInfo()] )
    station_n_ = stations_.size()
    station_index_seq_ = ee.List.sequence( 0, station_n_.add( -1 ) )
    
    def main( yr_mo_args ):
        yr_mo = ee.List( yr_mo_args )
        yr = ee.Number( yr_mo.get( 0 ) ).int()
        mo = ee.Number( yr_mo.get( 1 ) ).int()
        start_date = ee.Date.fromYMD( yr, mo, 1 )
        end_date = start_date.advance( 1, 'month' )
        ic = ee.ImageCollection( product_name_ )                                   \
                .filterDate( start_date, end_date )                                \
                .select( band_labels_[0] )
        
        def station_func( indexobj ):
            station_i = ee.Number( indexobj )
            station = ee.Feature( stations_list_.get( station_i ) )
            stationID = ee.Feature( stations_list_.get( station_i ) ).get( 'stationID' )
            ic_prop_list = ic.getRegion( ee.FeatureCollection( station ), 100 ).slice( 1 )
            station_data = ic_prop_list.map( get_column_from_nested_lists )
            max_precip = ee.Number( station_data.reduce( ee.Reducer.max() ) )
            return ee.Feature( None, {'system:time_start':start_date.millis(), 'stationID': stationID, 'precip':max_precip} )
        
        monthly_max_data = station_index_seq_.map( station_func )
        return monthly_max_data
    
    data_list = yr_mo_list.map( main )

    def organize_stationWise( indexobj ):
        station_i = ee.Number( indexobj )
        def get_feat_from_nested_lists( listobj ):
            feat_list = ee.List( listobj )
            feat = feat_list.get( station_i )
            return feat
        stationsdata = data_list.map( get_feat_from_nested_lists )
        return stationsdata

    stations_data = station_index_seq_.map( organize_stationWise )

    def reduce_data( indexobj ):
        station_i = ee.Number( indexobj )
        stationID = station_ids_strs_.get( station_i )
        station_data = ee.List( stations_data.get( station_i ) )
        station_fc = ee.FeatureCollection( station_data )
        def month_reduce( moobj ):
            mo = ee.Number( moobj ).add( 1 )
            month_filter = ee.Filter.calendarRange( mo, mo, 'month' )
            mo_fc = station_fc.filter( month_filter )
            stat_fc = mo_fc.reduceColumns( ee.Reducer.mean(), ['precip'] ).get( 'mean' )
            return stat_fc
        station_out = mo_index_seq_.map( month_reduce )
        return ee.Feature( None, {'stationID':stationID, 'statistic':station_out} )

    output_fc = ee.FeatureCollection( station_index_seq_.map( reduce_data ) )
    
    task = ee.batch.Export.table.toDrive( collection=output_fc, 
                                      description='GPM_Demo_2001_2021_{}'.format( str(batch_i) ),
                                      folder='GEE_Downloads',
                                      selectors=['system:index', 'stationID', 'statistic'] )
    
    task.start()
    
    while task.active():
        time.sleep( 30 )
    if task.status()['state'] != 'COMPLETED':
        pass
    else:
        pass
    
    later = dt.datetime.now()
    print(str(later - now))
    
    
    



    






