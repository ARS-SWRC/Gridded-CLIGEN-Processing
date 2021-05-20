
import ee
import datetime as dt
import time
import math

now = dt.datetime.now()

#ee.Authenticate()
ee.Initialize()

product_name_ = 'NASA/GPM_L3/IMERG_V06' 
band_labels_ = ['precipitationCal']
asset = ee.FeatureCollection( 'users/andrewfullhart/KGZ_TJK_GPM_Grid' )

start = '2001-01-01'
end = '2021-01-01'

stations_per_batch = 20

stations = ee.FeatureCollection( asset )

'slice stations in case of restart'
#station_feats = stations.toList( 10000000 )
#stations = ee.FeatureCollection( station_feats.slice( 320 ) )

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
    station_ids = ee.List( stations_.reduceColumns( ee.Reducer.toList(), ['stationID'] ).get( 'list' ) )
    station_ids_strs_ = ee.List( [str(elem) for elem in station_ids.getInfo()] )
    station_n_ = stations_.size()
    station_index_seq_ = ee.List.sequence( 0, station_n_.add( -1 ) )
    
    def get_column_from_nested_lists( list_obj ):
        data_list = ee.List( list_obj )
        data = ee.Number( data_list.get( 4 ) )
        return data
    
    def main( yr_mo_args ):
        yr_mo = ee.List( yr_mo_args )
        yr = ee.Number( yr_mo.get( 0 ) ).int()
        mo = ee.Number( yr_mo.get( 1 ) ).int()
        start_date = ee.Date.fromYMD( yr, mo, 1 )
        end_date = start_date.advance( 1, 'month' )
        ic = ee.ImageCollection( product_name_ )                                   \
                .filterDate( start_date, end_date )                                \
                .select( band_labels_[0] )
        ic_prop_list = ic.getRegion( ee.FeatureCollection( stations_ ), 100 ).slice( 1 )     
        data = ic_prop_list.map( get_column_from_nested_lists )
        n_data_per_station = data.length().divide( station_n_ ).int()
        def station_funcs( station_n ):
            interval = ee.Number( station_n )
            station_data = data.slice( n_data_per_station.multiply( interval ), 
                                        n_data_per_station.multiply( interval ).         
                                        add( n_data_per_station ) )
            max_precip = ee.Number( station_data.reduce( ee.Reducer.max() ) )
            return max_precip
        max_data = station_index_seq_.map( station_funcs )
        return max_data
    
    def disorganized_output_from_max_data( nested_list_obj ):
        max_data = ee.List( nested_list_obj )
        def mx5p_from_max_data( mo_int_obj ):
            mo_i = ee.Number( mo_int_obj ).int()
            def data_intervals( yr_int_obj ):
                yr_i = ee.Number( yr_int_obj ).int()
                mo_data = max_data.get( mo_i.add( yr_i.multiply( 12 ) ) )
                return mo_data    
            yr_lists = yr_index_seq_.map( data_intervals )
            return yr_lists
        output = mo_index_seq_.map( mx5p_from_max_data )
        return output
    
    def organized_output_from_max_data( mo_int_obj ):
        mo_i = ee.Number( mo_int_obj ).int()
        mo_data = ee.List( intermediate_org_.get( mo_i ) )
        def station_iteration( station_int_obj ):
            station_i = ee.Number( station_int_obj ).int()
            def year_iteration( yr_int_obj ):
                yr_i = ee.Number( yr_int_obj ).int()
                value = ee.Number( ee.List( mo_data.get( yr_i ) ).get( station_i ) ).float()
                return value
            station_list = ee.List( yr_index_seq_.map( year_iteration ) )
            mean = ee.Number( station_list.reduce( ee.Reducer.mean() ) ).float() #.multiply( 1.993 )
            return mean
        station_means = station_index_seq_.map( station_iteration )
        return station_means
    
    def make_fc( int_obj ):
        station_i = ee.Number( int_obj ).int()
        data = output_.get( station_i )
        dct = {'stationID': station_ids_strs_.get( station_i ),
               'MX5P': data}
        return ee.Feature( None, dct )
    
    
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
    
    disorganized_list = ee.List( yr_mo_list.map( main ) )
    
    intermediate_org_ = disorganized_output_from_max_data( disorganized_list )
    organized_output = mo_index_seq_.map( organized_output_from_max_data )
    output_ = ee.List( ee.Array( organized_output ).transpose().toList() )
    output_fc = ee.FeatureCollection( station_index_seq_.map( make_fc ) )
    
    task = ee.batch.Export.table.toDrive( collection=output_fc, 
                                      description='KGZ_TJK_GPM_2001_2021_{}'.format( str(batch_i) ),
                                      folder='GEE_Downloads' )
    
    task.start()
    
    while task.active():
        time.sleep( 30 )
    if task.status()['state'] != 'COMPLETED':
        pass
    else:
        pass
    
    later = dt.datetime.now()
    print(str(later - now))
    
    
    



