# import pymysql as sql
import DataCruiserLib.yaml_functions as pl


import DataCruiserLib.yaml_functions as yl
from DataCruiserLib.db_functions import DB_connection
import pandas as pd

def select_data(the_day):

    conn = DB_connection('local_mysql_woxi_data')
    if the_day.upper() == 'LATEST':
        day_list = get_list_of_days()
        the_day = day_list.max()[0]

    sql_str = ("SELECT DATETIME, LAT,LON, BSP, VMG, HDG, TWA FROM YACHT_DATA.woxi_data "
                " WHERE LAT is not null "
                "and DATE_FORMAT(DATETIME,'%Y-%m-%d') = '{0}' order by DATETIME".format(the_day) )

    # sql_str = ("SELECT DATETIME, LAT,LON, BSP, VMG, HDG, TWA FROM YACHT_DATA.woxi_data "
    #             " WHERE LAT is not null "
    #             "and DATETIME BETWEEN '{0} 00:00:00.0' AND '{0} 23:59:59.999' order by DATETIME".format(the_day) )



    df = pd.read_sql(sql_str, conn) #, index_col = 'THEDATETIME')
    conn.close()
    df.to_csv('dataframe')
    return(df)

def get_list_of_days():

    ### gets a list of values of unique days in DB

    conn = DB_connection('local_mysql_woxi_data')

    sql_str = "select DISTINCT(DATE_FORMAT(datetime,'%Y-%m-%d')) 'DAT' from woxi_data order by DATE_FORMAT(datetime,'%Y-%m-%d')"


    df = pd.read_sql(sql_str, conn) #, index_col = 'THEDATETIME')

    conn.close()
    return(df)


#####
#
#   specific data recovery routines
#
####

def get_date_list():
    # get data
    date_df = get_list_of_days()
    # format data
    list_dic_of_dates = []
    for dat in date_df.DAT:
        list_dic_of_dates.append({'label': dat, 'value' : dat})

    # list_dics_of_date = [
    #     {'label':'Today', 'value':'now'},
    #     {'label':'Yesterday','value':'past'},
    # ]
    return(list_dic_of_dates)

def calc_df_timestep(df):  # TODO add caclulation to find timedelta in seconds for 3rd to 4th records
    return(1)


def write_state_to_file(filename, state_dic):
    yl.write_yaml(filename ,state_dic )
    return(0)

def read_state_file(filename):
    data_dic = yl.open_yaml(filename)
    return (data_dic)

def get_time(data_frame):
    datetime = data_frame.loc[:, 'DATETIME']
    # may need to covnert to list
    return(datetime)


def get_lats(data_frame):
    if type(data_frame) is not pd.core.frame.DataFrame:
        lat = [data_frame.loc['LAT']]
    else:
        lat = data_frame.loc[:, 'LAT']
    # may need to covnert to list
    return(lat)

def get_lons(data_frame):
    if type(data_frame) is not pd.core.frame.DataFrame:
        lon = [data_frame.loc['LON']]
    else:
        lon = data_frame.loc[:, 'LON']
    # may need to covnert to list
    return(lon)

def get_lat_center(data_frame):
    lat = data_frame.loc[:, 'LAT']
    max_lat = max(lat)
    min_lat = min(lat)
    return (max_lat + min_lat)/2.0

def get_lon_center(data_frame):
    lon = data_frame.loc[:, 'LON']
    max_lon = max(lon)
    min_lon = min(lon)
    return (max_lon + min_lon)/2.0

def get_bsp(data_frame):
     return( data_frame.loc[:,'BSP'])

def get_vmg(data_frame):
    return (data_frame.loc[:, 'VMG'])

def get_twa(data_frame):
    return (data_frame.loc[:, 'TWA'])

def get_hdg(data_frame):
    return (data_frame.loc[:, 'HDG'])


