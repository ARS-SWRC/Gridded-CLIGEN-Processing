
import time
import ee
import datetime as dt
import math
now = dt.datetime.now()

#ee.Authenticate()
ee.Initialize()

product_name = 'NASA/GLDAS/V021/NOAH/G025/T3H'
band_labels = ['SWdown_f_tavg']
asset = 'users/andrewfullhart/PimaSantaCruz_ERA_Grid'
start = '2000-01-02'
end = '2020-01-01'

#convert_unit_factor_ = (3.*3600.) / 41840. 

stations_per_batch = 40

stations = ee.FeatureCollection( asset )

'slice stations in case of restart'
#station_feats = stations.toList( 10000000 )
#stations = ee.FeatureCollection( station_feats.slice( 0, 3 ) )

stationIDs = ee.List( stations.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )
stations_list_ = stations.toList( 1000000 )

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

strs_of_stats_ = ee.List( ['mean', 'stdDev'] )
number_of_stats_seq_ = ee.List.sequence( 0, strs_of_stats_.length().add( -1 ) )
months_seq = ee.List.sequence( 1, 12 )

start_date = dt.datetime.strptime( start, '%Y-%M-%d' )
end_date = dt.datetime.strptime( end, '%Y-%M-%d' )
diff = abs((end_date - start_date).days)

monthly_nested_dates_ = []
for i in range(12):
    mo_dates = []
    for j in range(diff):
        date = dt.timedelta( days=j ) + start_date
        if date.month == i+1:
            eedate = ee.Date( dt.datetime.strftime( date, '%Y-%m-%d' ) )
            mo_dates.append( eedate )
        else:
            pass
        
    monthly_nested_dates_.append( ee.List( mo_dates ) )

monthly_ee_dates_ = ee.List( monthly_nested_dates_ )

def region_array_to_feats( img_vals ):
    val_list = ee.List( img_vals )
    dct = {'system:time_start':ee.Date( val_list.get( 3 ) ).millis(),
            'SWin': ee.Number( val_list.get( 4 ) )}
    return ee.Feature( None, dct )

def dict_list_unpacker( dct_obj ):
    dct = ee.Dictionary( dct_obj )
    return dct.values()

def list_unpacker( eelist ):
    eelist = ee.List( eelist )
    return eelist.get( 4 )


for batch_i in range(batch_ct):
    stationID_batch_list = ee.List( station_groups_.get( batch_i ) )
    batch_filter = ee.Filter.inList( 'stationID', stationID_batch_list )
    stations_ = stations_list_.filter( batch_filter )
    stations_n_ = stations_.length()

    def main_funcs_caller( moobj ):
        mo = ee.Number( moobj )
        dates = ee.List( monthly_ee_dates_.get( mo.add( -1 ) ) )

        def date_func( dateobj ): 
            date = ee.Date( dateobj )
            ic = ee.ImageCollection( product_name )                                                                                                                                                                             \
                    .filterDate( date, date.advance( 1, 'day' ) )                 \
                    .select( band_labels[0] )

            def station_func( stationobj ):
                station = ee.Feature( stationobj )
                raw_data_list = ic.getRegion( station.geometry(), 1000 ).slice( 1 ) 
                data_list = raw_data_list.map( list_unpacker )
                total = ee.Number( data_list.reduce( ee.Reducer.sum() ) )
                return total
  
            out_list = stations_.map( station_func )            
            return out_list

        out_list = dates.map( date_func )

        def stats_func( indexobj ):
            index = ee.Number( indexobj )

            def column_list_func( listobj ):
                eeList = ee.List( listobj )
                eeNum = ee.Number( eeList.get( index ) )
                return eeNum

            station_data = out_list.map( column_list_func )                
            mean = ee.Number( station_data.reduce( ee.Reducer.mean() ) )
            stdDev = ee.Number( station_data.reduce( ee.Reducer.sampleStdDev() ) )
            return ee.List( [mean, stdDev] )

        out_stats = ee.List.sequence( 0, stations_n_.add( -1 ) ).map( stats_func )
        return out_stats

    out_list = months_seq.map( main_funcs_caller )

    def organize_output_func( stationindexobj ):
        stationIndex = ee.Number( stationindexobj )
        station_str = ee.String( ee.Feature( stations_.get( stationIndex ) ).get( 'stationID' ) )
        
        def stat_iter_func( statindexobj ):
            statIndex = ee.Number( statindexobj )
            
            def month_iter_func( moobj ):
                mo = ee.Number( moobj )                
                value = ee.List( ee.List( out_list.get( mo.add( -1 ) ) ).get( stationIndex ) ).get( statIndex )            
                return value

            out = months_seq.map( month_iter_func )
            station_stat_str = station_str.cat( '_' ).cat( strs_of_stats_.get( statIndex ) )
            return ee.Feature( None, {'station_ID': station_stat_str, 'statistic': out} )
    
        out = number_of_stats_seq_.map( stat_iter_func )
        return out
        
    out_list = ee.List.sequence( 0, stations_n_.add( -1 ) ).map( organize_output_func )
    out_list = out_list.flatten()
    out_fc = ee.FeatureCollection( out_list )

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


