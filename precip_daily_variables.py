
import ee
import datetime as dt
import time
import math
now = dt.datetime.now()

#ee.Authenticate()
ee.Initialize()


product_name = 'ECMWF/ERA5/DAILY'
band_labels = ['total_precipitation']
asset = 'users/andrewfullhart/PimaSantaCruz_ERA_Grid'
start = '2000-01-01'
end = '2020-01-01'

stations_per_batch = 40

stations = ee.FeatureCollection( asset )

'slice stations in case of restart'
#station_feats = stations.toList( 10000000 )
#stations = ee.FeatureCollection( station_feats.slice( 100 ) )


def dates_list_to_feats( date ):
    dct = {'system:time_start': ee.Date( date ).millis()}
    return ee.Feature( None, dct )

def region_array_to_feats( img_vals ):
    val_list = ee.List( img_vals )
    dct = {'system:time_start':ee.Date( val_list.get( 3 ) ).millis(),
            'precip': ee.Number( val_list.get( 4 ) )}
    return ee.Feature( None, dct )

def dict_list_unpacker( dct_obj ):
    dct = ee.Dictionary( dct_obj )
    return dct.values()

def list_flatten( list_obj ):
    return ee.List( list_obj ).flatten()


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

dates = []
start_date = ee.Date( start )
end_date = ee.Date( end )
n = end_date.difference( start_date, 'day' )
for i in range(n.getInfo()):
    date = start_date.advance( ee.Number( i ), 'day' )
    dates.append( date )

dates_ee = ee.List( dates )
dates_fc_ = ee.FeatureCollection( dates_ee.map( dates_list_to_feats ) )

months_seq = ee.List.sequence( 1, 12 )

strs_of_stats_ = ee.List( ['mean', 'stdDev', 'skew', 'pWD', 'pWW'] )
number_of_stats_seq_ = ee.List.sequence( 0, strs_of_stats_.length().add( -1 ) )

for batch_i in range(batch_ct):
    stationID_batch_list = ee.List( station_groups_.get( batch_i ) )
    batch_filter = ee.Filter.inList( 'stationID', stationID_batch_list )
    stations_ = stations.filter( batch_filter )
    station_ids = ee.List( stations_.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )
    station_ids_strs_ = ee.List( [str(elem) for elem in station_ids.getInfo()] )
    
    def main_funcs_caller( mo ):
        month_filter = ee.Filter.calendarRange( mo, mo, 'month' )
        ic = ee.ImageCollection( product_name )                                    \
                .filterDate( start, end )                                          \
                .filter( month_filter )                                            \
                .select( band_labels[0] )
    
        def station_funcs( station ):
            ic_prop_array = ic.getRegion( ee.FeatureCollection( station ), 1000 ).slice( 1 )     
            day_vals_fc = ee.FeatureCollection( ic_prop_array.map( region_array_to_feats ) )
            day_vals = day_vals_fc.reduceColumns( ee.Reducer.toList(), ['precip'] ).get( 'list' )
            
            def prob_counter( precip_list_obj ):
                precip_list = ee.List( precip_list_obj )
                first = precip_list.get( 0 )
                counts = ee.List( [0, 0, 0, 0] )
                first_data = ee.List( [first, counts] )
            
                def begin_counter( curr, prev_data ):
                    curr_num = ee.Number( curr ).float()
                    data = ee.List( prev_data )
                    prev_num = ee.Number( data.get( 0 ) ).float()
                    prev_counts = ee.List( data.get( 1 ) )
                    ww_ct = ee.Number( prev_counts.get( 0 ) ).int()
                    wd_ct = ee.Number( prev_counts.get( 1 ) ).int()
                    dw_ct = ee.Number( prev_counts.get( 2 ) ).int()
                    dd_ct = ee.Number( prev_counts.get( 3 ) ).int()                        
                    out = ee.Algorithms.If( curr_num.gt( 0. ), 
                                            ee.Algorithms.If( prev_num.gt( 0. ), 
                                                              ee.List( [curr_num, ee.List( [ww_ct.add( 1 ), wd_ct, dw_ct, dd_ct] ) ] ), 
                                                              ee.List( [curr_num, ee.List( [ww_ct, wd_ct.add( 1 ), dw_ct, dd_ct] ) ] ) ),
                                            ee.Algorithms.If( prev_num.gt( 0. ), 
                                                              ee.List( [curr_num, ee.List( [ww_ct, wd_ct, dw_ct.add( 1 ), dd_ct] ) ] ), 
                                                              ee.List( [curr_num, ee.List( [ww_ct, wd_ct, dw_ct, dd_ct.add( 1 )] ) ] ) ) )
                    return ee.List( out )
            
                iter_out = ee.List( precip_list.iterate( begin_counter, first_data ) )
                return ee.List( iter_out )
    
            nonzeros_fc = day_vals_fc.filter( ee.Filter.gt( 'precip', ee.Number( 0 ) ) )
            mean = nonzeros_fc.reduceColumns( ee.Reducer.mean(), ['precip'] )
            stdDev = nonzeros_fc.reduceColumns( ee.Reducer.sampleStdDev(), ['precip'] )
            skew = nonzeros_fc.reduceColumns( ee.Reducer.skew(), ['precip'] )
            counts = ee.List( prob_counter( day_vals ).get( 1 ) )
            ww_ct = ee.Number( counts.get( 0 ) ).float()
            wd_ct = ee.Number( counts.get( 1 ) ).float()
            dw_ct = ee.Number( counts.get( 2 ) ).float()
            dd_ct = ee.Number( counts.get( 3 ) ).float()
            wd = wd_ct.divide( wd_ct.add( dd_ct ) )
            ww = ww_ct.divide( dw_ct.add( ww_ct ) )
            dct = {'mean': mean, 'stdDev': stdDev, 'skew': skew,
                    'pWD': {'pWD': wd}, 'pWW': {'pWW': ww}}
            return ee.Feature( None, dct )
    
        out_fc = ee.FeatureCollection( stations_.map( station_funcs ) )
        mean = out_fc.reduceColumns( ee.Reducer.toList(), ['mean'] ).get( 'list' )
        stdDev = out_fc.reduceColumns( ee.Reducer.toList(), ['stdDev'] ).get( 'list' )
        skew = out_fc.reduceColumns( ee.Reducer.toList(), ['skew'] ).get( 'list' )
        pWD = out_fc.reduceColumns( ee.Reducer.toList(), ['pWD'] ).get( 'list' )
        pWW = out_fc.reduceColumns( ee.Reducer.toList(), ['pWW'] ).get( 'list' )
        out_list = ee.List( [] )
        out_list = out_list.add( ee.List( mean ).map( dict_list_unpacker ) )
        out_list = out_list.add( ee.List( stdDev ).map( dict_list_unpacker ) )
        out_list = out_list.add( ee.List( skew ).map( dict_list_unpacker ) )
        out_list = out_list.add( ee.List( pWD ).map( dict_list_unpacker ) )
        out_list = out_list.add( ee.List( pWW ).map( dict_list_unpacker ) )
        return out_list
        
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
    
    out_list = months_seq.map( main_funcs_caller )
    out_array = ee.Array( out_list ).transpose()
    out_list_ = out_array.toList()
    
    out_fc = ee.FeatureCollection( station_ids_strs_.map( out_list_unpacker ) ).flatten()
    
    task = ee.batch.Export.table.toDrive( collection=out_fc, 
                                      description='ERA_Demo_2000_2020_{}'.format( str(batch_i) ),
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
    
