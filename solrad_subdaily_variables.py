
import ee
import datetime as dt
import time
import math
now = dt.datetime.now()

#ee.Authenticate()
ee.Initialize()


product_name = 'NASA/GLDAS/V021/NOAH/G025/T3H'
band_labels = ['SWdown_f_tavg']
asset = ee.FeatureCollection( 'users/andrewfullhart/PimaSantaCruz_ERA_Grid' )
start = '2000-01-01'
end = '2020-01-01'

#convert_unit_factor_ = (3.*3600.) / 41840. 

stations_per_batch = 40

stations = ee.FeatureCollection( asset )

'slice stations in case of restart'
#station_feats = stations.toList( 10000000 )
#stations = ee.FeatureCollection( station_feats.slice( 0, 3 ) )

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

def dates_list_to_feats( date ):
    dct = {'system:time_start': ee.Date( date ).millis()}
    return ee.Feature( None, dct )

def region_array_to_feats( img_vals ):
    val_list = ee.List( img_vals )
    dct = {'system:time_start':ee.Date( val_list.get( 3 ) ).millis(),
            'SWin': ee.Number( val_list.get( 4 ) )}
    return ee.Feature( None, dct )

def dict_list_unpacker( dct_obj ):
    dct = ee.Dictionary( dct_obj )
    return dct.values()

for batch_i in range(batch_ct):
    stationID_batch_list = ee.List( station_groups_.get( batch_i ) )
    batch_filter = ee.Filter.inList( 'stationID', stationID_batch_list )
    stations_ = stations.filter( batch_filter )
    
    def main_funcs_caller( mo ):
        month_filter = ee.Filter.calendarRange( mo, mo, 'month' )
        ic = ee.ImageCollection( product_name )                                    \
                .filterDate( start, end )                                          \
                .filter( month_filter )                                            \
                .select( band_labels[0] )
    
        def station_funcs( station ):
            ic_prop_array = ic.getRegion( ee.FeatureCollection( station ), 1000 ).slice( 1 )     
            raw_data_fc = ee.FeatureCollection( ic_prop_array.map( region_array_to_feats ) )
    
            def daily_sorter( day ):
                start = ee.Date( day.get( 'system:time_start' ) )
                end = start.advance( 1, 'day' )
                day_filter = ee.Filter.date( start, end )
                fc = raw_data_fc.filter( day_filter )
                total = fc.reduceColumns( ee.Reducer.sum(), ee.List( ['SWin'] ) )
                acc = ee.Number( total.get( 'sum' ) )#.multiply( convert_unit_factor_ )
                return ee.Feature( None, {'SWin': acc} )
    
            day_dates_fc = dates_fc_.filter( month_filter )
            day_vals_fc = ee.FeatureCollection( day_dates_fc.map( daily_sorter ) )
            mean = day_vals_fc.reduceColumns( ee.Reducer.mean(), ['SWin'] )
            stdDev = day_vals_fc.reduceColumns( ee.Reducer.sampleStdDev(), ['SWin'] )

            dct = {'mean': mean, 'stdDev': stdDev}
            return ee.Feature( None, dct )

        out_fc = ee.FeatureCollection( stations_.map( station_funcs ) )
        mean = out_fc.reduceColumns( ee.Reducer.toList(), ['mean'] ).get( 'list' )
        stdDev = out_fc.reduceColumns( ee.Reducer.toList(), ['stdDev'] ).get( 'list' )
        out_list = ee.List( [] )
        out_list = out_list.add( ee.List( mean ).map( dict_list_unpacker ) )
        out_list = out_list.add( ee.List( stdDev ).map( dict_list_unpacker ) )
        return out_list

    def list_flatten( list_obj ):
        return ee.List( list_obj ).flatten()
    
    def out_list_unpacker( station_id ):
        station_str = ee.String( station_id )
        station_i = station_ids_strs_.indexOf( station_str )
        def stat_features( nested_i ):
            nested_i = ee.Number( nested_i ).int()
            nested_i_list = ee.Array( out_list_.get( nested_i ) ).transpose().toList()
            nested_i_list = nested_i_list.map( list_flatten ).get( station_i )
            station_stat_str = station_str.cat( '_' ).cat( strs_of_stats_.get( nested_i ) )
            dct = {'station_ID': station_stat_str,
                    'statistic': nested_i_list}
            return ee.Feature( None, dct )
        fc = ee.FeatureCollection( number_of_stats_seq_.map( stat_features ) )
        return fc
    
    dates = []
    start_date = ee.Date( start )
    end_date = ee.Date( end )
    n = end_date.difference( start_date, 'day' )
    for i in range(n.getInfo()):
        date = start_date.advance( ee.Number( i ), 'day' )
        dates.append( date )
    dates_ee = ee.List( dates )
    dates_fc_ = ee.FeatureCollection( dates_ee.map( dates_list_to_feats ) )
    
    station_ids = ee.List( stations_.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )
    station_ids_strs_ = ee.List( [str(elem) for elem in station_ids.getInfo()] )
    
    months_seq = ee.List.sequence( 1, 12 )
    
    strs_of_stats_ = ee.List( ['mean', 'stdDev'] )
    number_of_stats_seq_ = ee.List.sequence( 0, strs_of_stats_.length().add( -1 ) ) 
    
    out_list = months_seq.map( main_funcs_caller )
    out_array = ee.Array( out_list ).transpose()
    out_list_ = out_array.toList()
    
    out_fc = ee.FeatureCollection( station_ids_strs_.map( out_list_unpacker ) ).flatten()
    
    task = ee.batch.Export.table.toDrive( collection=out_fc, 
                                      description='GLDAS_DEMO_2000_2020_{}'.format( str(batch_i) ),
                                      folder='GEE_Downloads',
                                      selectors=['system:index', 'station_ID', 'statistic'] )

    task.start()
    
    while task.active():
        time.sleep( 30 )
    if task.status()['state'] != 'COMPLETED':
        pass
    else:
        pass
    
    later = dt.datetime.now()
    print(str(later - now))
    
