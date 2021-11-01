#GO TO LINE 36: Check path to geometry asset. E.g. change to 'users/andrewfullhart/Africa_ERA_Grid'.                    
#GO TO LINES 45,46: If slicing, uncomment and enter slice indices (start index is inclusive, end index is exclusive).
#GO TO LINE 143: Check name of Google Drive export folder.
import ee
import datetime as dt
import time
import math
import sys



def region_array_to_feats( img_vals ):
    val_list = ee.List( img_vals )
    maxtemp = ee.Number( val_list.get( 4 ) )
    mintemp = ee.Number( val_list.get( 5 ) )
    dewtemp = ee.Number( val_list.get( 6 ) )

    dct = {'system:time_start':ee.Date( val_list.get( 3 ) ).millis(),
            'maximum': maxtemp,
            'minimum': mintemp,
            'dewpoint': dewtemp}
    return ee.Feature( None, dct )

def dict_list_unpacker( dct_obj ):
    dct = ee.Dictionary( dct_obj )
    return dct.values()

def main(): 
    now = dt.datetime.now()
    
    #ee.Authenticate()
    ee.Initialize()
    
    product_name_ = 'ECMWF/ERA5/DAILY'
    band_labels_ = ['maximum_2m_air_temperature', 'minimum_2m_air_temperature', 'dewpoint_2m_temperature']
    asset = ee.FeatureCollection( 'users/andrewfullhart/PimaSantaCruz_ERA_Grid' )
    start_ = '2000-01-01'
    end_ = '2020-01-01'
    
    stations_per_batch = 40
    
    stations = ee.FeatureCollection( asset )
    
    'slice stations in case of restart'
    #station_feats = stations.toList( 10000000 )
    #stations = ee.FeatureCollection( station_feats.slice( 3000, 3003 ) )
    
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

    for batch_i in range(batch_ct):
        stationID_batch_list = ee.List( station_groups_.get( batch_i ) )
        batch_filter = ee.Filter.inList( 'stationID', stationID_batch_list )
        stations_ = stations.filter( batch_filter )
    
        def main_funcs_caller( mo ):
            month_filter = ee.Filter.calendarRange( mo, mo, 'month' )
            ic = ee.ImageCollection( product_name_ )                                      \
                    .filterDate( start_, end_ )                                           \
                    .filter( month_filter )                                               \
                    .select( band_labels_ )
        
            def station_funcs( station ):
                ic_prop_array = ic.getRegion( station.geometry(), 1000 ).slice( 1 )
                
                raw_data_fc = ee.FeatureCollection( ic_prop_array.map( region_array_to_feats ) )
                
                maxMean = raw_data_fc.reduceColumns( ee.Reducer.mean(), ['maximum'] )
                maxStdDev = raw_data_fc.reduceColumns( ee.Reducer.sampleStdDev(), ['maximum'] )
                minMean = raw_data_fc.reduceColumns( ee.Reducer.mean(), ['minimum'] )
                minStdDev = raw_data_fc.reduceColumns( ee.Reducer.sampleStdDev(), ['minimum'] )
                dewMean = raw_data_fc.reduceColumns( ee.Reducer.mean(), ['dewpoint'] )
                
                dct = {'Tmax':maxMean, 'TmaxStdDev':maxStdDev, 'Tmin':minMean, 
                       'TminStdDev':minStdDev, 'Tdew':dewMean}
                
                return ee.Feature( None, dct )        
    
            out_fc = ee.FeatureCollection( stations_.map( station_funcs ) )        
            mean = out_fc.reduceColumns( ee.Reducer.toList(), ['Tmax'] ).get( 'list' )
            stdDev = out_fc.reduceColumns( ee.Reducer.toList(), ['TmaxStdDev'] ).get( 'list' )
            skew = out_fc.reduceColumns( ee.Reducer.toList(), ['Tmin'] ).get( 'list' )
            pWD = out_fc.reduceColumns( ee.Reducer.toList(), ['TminStdDev'] ).get( 'list' )
            pWW = out_fc.reduceColumns( ee.Reducer.toList(), ['Tdew'] ).get( 'list' )
            out_list = ee.List( [] )
            out_list = out_list.add( ee.List( mean ).map( dict_list_unpacker ) )
            out_list = out_list.add( ee.List( stdDev ).map( dict_list_unpacker ) )
            out_list = out_list.add( ee.List( skew ).map( dict_list_unpacker ) )
            out_list = out_list.add( ee.List( pWD ).map( dict_list_unpacker ) )
            out_list = out_list.add( ee.List( pWW ).map( dict_list_unpacker ) )
    
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
        
        station_ids = ee.List( stations_.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )
        station_ids_strs_ = ee.List( [str(elem) for elem in station_ids.getInfo()] )
        
        months_seq = ee.List.sequence( 1, 12 )
        
        strs_of_stats_ = ee.List( ['Tmax', 'TmaxStdDev', 'Tmin', 'TminStdDev', 'Tdew'] )
        number_of_stats_seq_ = ee.List.sequence( 0, strs_of_stats_.length().add( -1 ) ) 
        
        out_list = months_seq.map( main_funcs_caller )
        
        out_array = ee.Array( out_list ).transpose()
        out_list_ = out_array.toList()
        
        out_fc = ee.FeatureCollection( station_ids_strs_.map( out_list_unpacker ) ).flatten()
        
        task = ee.batch.Export.table.toDrive( collection=out_fc, 
                                          description='ERA_Temps_2000_2020_{}'.format( str(batch_i) ),
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

if __name__ == '__main__':
    sys.exit( main() )


