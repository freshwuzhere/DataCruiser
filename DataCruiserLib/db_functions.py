# import db_constants as db_C
# from my_tools import to_datetime
# import scipy.interpolate as sp
# import numpy as np
# import sys
# import datetime

import pymysql as db
import DataCruiserLib.yaml_functions as yl
import pandas as pd




def DB_connection(db_choice):
    db_info = yl.open_yaml('/Users/ianburns/Documents/Code-Dev/PycharmProjects/MyDataFiles/db_constants.yaml')
    login_info = db_info['db_login'][db_choice]
    conn = db.connect(host=login_info['MYSQL_DB_IP'],
                      port=login_info['MYSQL_DB_PORT'],
                      user=login_info['MYSQL_DB_USER'],
                      passwd=login_info['MYSQL_DB_PWD'],
                      db=login_info['MYSQL_DB_DB_CHOICE'])
    return(conn)


def load_a_dataframe(conn, start_time, end_time):

    sql_str = "select DATETIME, LAT, LON, TWS, BSP, TWA, VMG from woxi_data where DATETIME BETWEEN '{0}' and '{1}' ".format(start_time.strftime('%Y-%m-%d %H:%M:%S')  ,end_time.strftime('%Y-%m-%d %H:%M:%S') )

    data_frame = pd.read_sql(sql_str, conn , index_col='DATETIME')

    return(data_frame)

# def get_event_attributes(conn, start_date_str, end_date_str, event_type , sql_str ,boats = ['AC50_1'] , sql_where = ' ',  r_or_t_number=None , TIME_OR_TIME_START = ' TIME_START '):
#
#     table_name = 'ATTRIBUTES_FOR_' + event_type
#     event_type_shrt = event_type.split('_')[1]
#     # Need to add boat or any
#     boat_list_str = ''
#
#     for boat in boats:
#         boat_list_str = boat_list_str + ' , ' + "'" + boat + "'"
#
#     boat_list_str  = boat_list_str[2:]
#
#
#     if r_or_t_number == None :
#         sql_string = ("SELECT {0} FROM EVENTS EV LEFT JOIN {1} B ON EV.ID = B.FK_EVENTS_ID "
#                       " LEFT JOIN EVENT_SUBTYPE C ON C.ID = EV.FK_EVENT_SUBTYPE_ID "
#                       " LEFT JOIN PARTICIPANTS PTC ON EV.FK_PARTICIPANTS_ID = PTC.ID "
#                       " LEFT JOIN participants_members PTC_MEM on PTC.ID = PTC_MEM.FK_PARTICIPANTS_ID "
#                       " LEFT JOIN DATASOURCE DS ON DS.ID = PTC_MEM.FK_DATASOURCE_ID "
#                       " WHERE {5} BETWEEN '{2}'  AND  '{3}'"
#                       + sql_where +
#                       " AND C.NAME = '{4}'"
#                       " AND DS.NAME IN ( {6} )").format(sql_str, table_name, start_date_str, end_date_str, event_type_shrt , TIME_OR_TIME_START , boat_list_str)
#
#
#     else:
#         for r_or_t in r_or_t_number:
#             #start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
#             #end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
#             if event_type.upper() == 'RANGE_TEST':
#                 race_or_test_str = ' and TEST_NUMBER = ' + str(r_or_t)
#             table_name = 'ATTRIBUTES_FOR_' + event_type
#             sql_string = (  "SELECT {0} FROM EVENTS EV LEFT JOIN   {1} B ON EV.ID = B.FK_EVENTS_ID "
#                             " LEFT JOIN EVENT_SUBTYPE C ON C.ID = EV.FK_EVENT_SUBTYPE_ID "
#                             " LEFT JOIN PARTICIPANTS PTC ON EV.FK_PARTICIPANTS_ID = PTC.ID "
#                             " LEFT JOIN participants_members PTC_MEM on PTC.ID = PTC_MEM.FK_PARTICIPANTS_ID "
#                             " LEFT JOIN DATASOURCE DS ON DS.ID = PTC_MEM.FK_DATASOURCE_ID "
#                             " WHERE {6} BETWEEN '{2}' "
#                             "AND  '{3}' {4}  AND C.NAME = '{5}'"
#                             " AND DS.NAME = '{6}' ").format( sql_str,table_name,start_date_str, end_date_str,race_or_test_str, event_type_shrt , TIME_OR_TIME_START , boat)
#
#
#     cur = conn.cursor()
#     cur.execute(sql_string)
#
#     return (load_arrays_from_cursor(cur,ret_as_list=False))
#
#
# def set_DB_value(conn, num_to_set, col, where_clause, tab ):
#     sql_str =  " UPDATE  {0} SET {1} = {2}  {3}".format(tab, col ,num_to_set, where_clause)
#     cur = conn.cursor()
#     try:
#         cur.execute(sql_str)
#     except Exception as e:
#         #e = sys.exc_info()[0]
#         print('PyMySQL exception  = ' + str(e))
#     return
#
#
# def get_event_attributes_from_within_events(conn,
#                                             start_date,
#                                             end_date,
#                                             sql_vars_str ,
#                                             boat_name,
#                                             datasource_freq,
#                                             datasource_type,
#                                             event_type_outer,
#                                             event_type_inner,
#                                             sql_where_clause = '' ):
#
#     # NOTES - could return attributes of tacks with legs or within races within a period - where sql could limit to upwind or down
#     # NOTES - Call events by 'RANGE_LEG'  or 'SINGLE_TACK'
#
#     event_type_inner_shrt = event_type_inner.split('_')[1]
#     event_type_outer_shrt = event_type_outer.split('_')[1]
#     # sql_vars_str =  'LEG_TIME.RN,LEG_TIME.LEG,  COUNT(IF(FOILING>96 ,1 , NULL))'
#
#     cur = conn.cursor()
#
#     # DO TWO LOOPS - FIRST THROUGH OUTER EVENTS - THEN INTERNALLY THROUGH INNER EVENTS.
#     inner_dat = []
#     outer_times = get_event_attributes(conn,start_date,end_date,event_type_outer,' time_start, time_end ' , boats = [boat_name])
#
#     if len(outer_times) < 1:
#         print('failed to find outer loop')
#         inner_dat = [None]
#     else:
#         for start, end in zip(outer_times['time_start'] , outer_times['time_end']):
#             in_ev_dat = get_event_attributes(conn, start,end, event_type_inner, sql_vars_str , boats = [boat_name])
#             inner_dat.append(in_ev_dat)
#
#     return(inner_dat)
#
# def get_data_from_summary_table(conn, event_type, table_type, sql_vars , ev_Id):
#     table_name = "summary_"+ event_type + '_AC45S_' + table_type
#
#     sql_str = (" SELECT {0} FROM {1} "
#                " WHERE FK_EVENTS_ID = {2} ").format(sql_vars , table_name , ev_Id)
#
#     cur = conn.cursor()
#     try:
#         cur.execute(sql_str)
#     except:
#         e = sys.exc_info()[0]
#         print(str(e))
#
#     ### Collect the data into arrays
#     temp_data = load_arrays_from_cursor(cur , ret_as_list = False)
#
#     return(temp_data)
#
#
# def get_data_from_tables_for_events(conn, start_date, end_date, sql_vars_str , boat_name, datasource_freq, datasource_type, event_type, sql_where_clause = '' ):
#     # NOTES - datasource_freq can be '2 HZ', '10 HZ' or '1 HZ'
#     #         event type is RANGE_LEG or SINGLE_TACK
#
#     event_type_shrt = event_type.split('_')[1]
#
#
#     cur = conn.cursor()
#
#     data_table_list = collect_data_table_names(conn, datasource_freq, datasource_type)
#
#     # sql_str = 'select EV.ID ' + sql_vars_str + data_table_list + " WHERE THEDATETIME BETWEEN '" + \
#     #           start_date + "' AND '" + end_date + "'" + " AND FK_DATASOURCE_ID = (SELECT ID FROM DATASOURCE WHERE NAME = '" + boat_name + "' )"  +  \
#     #           sql_where_clause
#
#     #  NOTE this is locked on segment borad
#     sql_str =   'select ' + sql_vars_str + data_table_list + \
#                 " inner join participants_members pm ON pm.FK_DATASOURCE_ID = A.FK_DATASOURCE_ID " \
#                 " inner join events EV ON A.thedatetime between EV.time_start and EV.time_end " \
#                 "  and EV.fk_event_subtype_id = (SELECT ID FROM EVENT_SUBTYPE WHERE NAME = '" + event_type_shrt + "') " \
#                 " and EV.FK_PARTICIPANTS_ID = pm.FK_PARTICIPANTS_ID left join attributes_for_range_segmentboard attr " \
#                 "  ON attr.fk_events_id = EV.id " \
#                 " WHERE A.THEDATETIME BETWEEN '" + \
#                 start_date + "' AND '" + end_date + "'" + " AND A.FK_DATASOURCE_ID = (SELECT ID FROM DATASOURCE WHERE NAME = '" + boat_name + "' )"  +  \
#                 sql_where_clause
#
#     try:
#         cur.execute(sql_str)
#     except:
#         e = sys.exc_info()[0]
#         print(str(e))
#
#     ### Collect the data into arrays
#     name, temp_data = load_arrays_from_cursor(cur , ret_as_list = True)
#
#     return(name,temp_data)
#
#
#
#
# def get_data_from_tables(conn,
#                          start_date,
#                          end_date,
#                          sql_vars_str ,
#                          boat_list,
#                          datasource_freq,
#                          datasource_type,
#                          list_or_dict = 'list',
#                          sql_where_clause = '' ,
#                          sql_group_clause =  '' ,
#                          sql_order_clause = '' ):
#     # NOTES - datasource_freq can be '2 HZ', '10 HZ' or '1 HZ'
#
#     # format dates
#     start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
#     end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
#
#     boat_list_str = ""
#     for idx,boat in enumerate(boat_list):
#         if idx == 0:
#             boat_list_str = boat_list_str + " '{}' ".format(boat)
#         else:
#             boat_list_str = boat_list_str + ", '{}' ".format(boat)
#
#     cur = conn.cursor()
#
#     data_table_list = collect_data_table_names(conn, datasource_freq, datasource_type)
#
#     sql_str = 'select ' + sql_vars_str + data_table_list + " LEFT JOIN DATASOURCE DS  ON A.FK_DATASOURCE_ID = DS.ID  WHERE THEDATETIME BETWEEN '" + \
#               start_date_str + "' AND '" + end_date_str + "'" + " AND DS.NAME IN (" + boat_list_str + " ) "  +  \
#               sql_where_clause + sql_group_clause + sql_order_clause
#
#     try:
#         cur.execute(sql_str)
#     except Exception as e:
#         # e = sys.exc_info()[0]
#         print('PyMySQL exception  = ' + str(e))
#
#     ### Collect the data into arrays
#     if list_or_dict == 'dict':
#         temp_data = load_arrays_from_cursor(cur, ret_as_list= False)
#         return ( temp_data)
#
#     else:
#         name, temp_data = load_arrays_from_cursor(cur , ret_as_list = True)
#         return (name,temp_data)
#
#
#
#
# def collect_data_table_names(conn, datasource_freq, datasource_type):
#     sql_string = ("SELECT TABLE_NAME FROM DATA_TABLES WHERE FK_DATA_FREQ_ID= (SELECT ID FROM DATA_FREQ WHERE NAME =  "
#                   "'" + datasource_freq + "' "
#                   " AND FK_DATASOURCE_TYPE_ID = (SELECT ID FROM DATASOURCE_TYPE WHERE NAME = '" + datasource_type + "'))")
#
#     cur = conn.cursor()
#
#     cur.execute(sql_string)
#
#     dat_tables = cur.fetchall()
#
#     temp_table = dat_tables[0][0]
#     time_table = temp_table[:-5] + 'TIME'  # may nee T after name Time
#
#     sql_table_join = 'FROM ' + time_table + ' A'
#
#     for table in dat_tables:
#         temp_dat = table[0]
#         sql_table_join = sql_table_join + ' LEFT JOIN ' + temp_dat + " ON A.ID = " + temp_dat + ".FK_MAIN_TABLE_ID"
#
#     return (sql_table_join)
#
#
# def load_arrays_from_cursor(curs , ret_as_list = True):
#     # this takes a curs from DB and loads the data in to a list of dictionary (depeneding on ret_as_list - False = dictionary).
#     names = []
#     if curs.description !=  None :
#         for idx, col_name in enumerate(curs.description):
#             names.append(col_name[0])
#
#         results = curs.fetchall()
#
#         cols = zip(*results)
#
#         data_dict= {}
#         data_lists = []
#         for idx, col in enumerate(cols):
#             # data_lists.append(np.asarray(col))
#             data_lists.append(col)
#             data_dict[names[idx]] = list(col)
#         if ret_as_list:
#             return (names, data_lists)
#         else:
#             return( data_dict)
#     else:
#         return None
#
#
# def Boat_Paticipant_Group(conn, boat_name, group_size=1):
#     if group_size == 1:
#
#         boat_select_sql = 'SELECT ARB.PRT FROM (SELECT FK_PARTICIPANTS_ID PRT,FK_DATASOURCE_ID,COUNT(FK_PARTICIPANTS_ID) FROM PARTICIPANTS_MEMBERS ' \
#                           ' GROUP BY (FK_PARTICIPANTS_ID) HAVING COUNT(FK_PARTICIPANTS_ID) =1 ' \
#                           " AND FK_DATASOURCE_ID = (SELECT ID FROM DATASOURCE WHERE NAME = '" + boat_name + "')) ARB "
#
#     else:
#         # this is for larger groups.....  NOT WRITTEN yet.
#         pass
#
#     cur = conn.cursor()
#
#     cur.execute(boat_select_sql)
#
#     dat_tables = cur.fetchall()
#
#     id = dat_tables[0][0]
#
#     return (id)
#
#
# def get_names_from_participants(conn, part_group_number):
#     sql_var = ( "SELECT DS.NAME from DATASOURCE DS LEFT JOIN participants_members PM ON PM.FK_DATASOURCE_ID = DS.ID "
#                 "LEFT JOIN PARTICIPANTS PT ON PM.FK_PARTICIPANTS_ID = PT.ID WHERE PT.ID = " + str(part_group_number) )
#
#     curs = conn.cursor()
#
#     curs.execute(sql_var)
#
#     name_list = load_arrays_from_cursor(curs, ret_as_list=True)
#
#     return (name_list)
#
#
# def attributes_from_day(conn, day_str , attr_name ):
#
#     sql_string =  "SELECT * FROM EVENTS A  LEFT JOIN ATTRIBUTES_FOR_RANGE_" + attr_name + " B ON B.FK_EVENTS_ID = A.ID  " \
#             " where A.FK_EVENT_SUBTYPE_ID = (SELECT ID FROM EVENT_SUBTYPE WHERE NAME LIKE '" + attr_name + "')  AND  A.THEDATE = '"+ day_str + "'"
#
#     cur = conn.cursor()
#     cur.execute(sql_string)
#
#
#     dat_tables = load_arrays_from_cursor(cur , ret_as_list = False)
#
#     return(dat_tables)
#
#
# def get_event_attributes_from_race_leg(conn ,the_date, race_number, leg_number , sql_vars = ' * ', sql_where= ' '):
#     thedate_str = the_date.strftime('%Y/%m/%d')
#
#     sql_str =   (" SELECT * FROM racecutter.attributes_for_range_leg A  LEFT JOIN EVENTS B ON B.ID = A.FK_EVENTS_ID "
#                 " LEFT JOIN PARTICIPANTS D ON B.FK_PARTICIPANTS_ID = D.ID LEFT JOIN PARTICIPANTS_MEMBERS E ON E.FK_PARTICIPANTS_ID = D.ID "
#                 " LEFT JOIN DATASOURCE F ON F.ID = E.FK_DATASOURCE_ID WHERE B.THEDATE = '" + thedate_str  + " "
#                 "'  AND RACE_NUMBER=" + str(race_number) + " AND LEG = " + str(leg_number) + sql_where +"  "
#                 " ORDER BY THEDATE,RACE_NUMBER,F.NAME,LEG ")
#
#     cur = conn.cursor()
#     cur.execute(sql_str)
#
#
#     dat_tables = load_arrays_from_cursor(cur , ret_as_list = False)
#
#     return(dat_tables)
#
#
#     pass
#
#
# def get_list_of_races_participants(conn, start_date_str,end_date_str , race_number = None, races = True , legs = False , thedate = None):
#     if races:
#         group_by = ' GROUP BY THEDATE, RACE_NUMBER '
#     elif legs:
#         group_by = ' GROUP BY THEDATE, RACE_NUMBER , LEG '
#     else:
#         group_by = ' '
#
#     if race_number:
#         group_by = ' AND RACE_NUMBER = ' + str(race_number) + group_by
#
#     if thedate == None:
#         sql_query = (   "SELECT B.THEDATE, B.RACE_NUMBER RACE_NUMBER , A.LEG , B.FK_PARTICIPANTS_ID PARTICIPANTS FROM racecutter.attributes_for_range_leg A "
#                         " LEFT JOIN EVENTS B ON B.ID = A.FK_EVENTS_ID "
#                         " WHERE B.TIME_START > '" +start_date_str +"' AND  B.TIME_END < '" + end_date_str + "' "  + group_by + " "
#                         " ORDER BY THEDATE,RACE_NUMBER, LEG" )
#     else:
#         sql_query = (   "SELECT B.THEDATE, B.RACE_NUMBER RACE_NUMBER , A.LEG , B.FK_PARTICIPANTS_ID PARTICIPANTS FROM racecutter.attributes_for_range_leg A "
#                         " LEFT JOIN EVENTS B ON B.ID = A.FK_EVENTS_ID "
#                         " WHERE B.THEDATE =  '" + thedate + "' "  + group_by + " "
#                         " ORDER BY THEDATE,RACE_NUMBER, LEG" )
#
#
#
#     cur= conn.cursor()
#
#     cur.execute(sql_query)
#
#     dat_tables = load_arrays_from_cursor(cur , ret_as_list=False)
#
#     return( dat_tables)
#
#
# def get_list_of_races_participants_special(conn, start_date_str,end_date_str , race_number = None, races = True , legs = False):
#     if races:
#         group_by = ' GROUP BY THEDATE, RACE_NUMBER '
#     elif legs:
#         group_by = ' GROUP BY THEDATE, RACE_NUMBER , LEG '
#     else:
#         group_by = ' '
#
#     if race_number:
#         group_by = ' AND RACE_NUMBER = ' + str(race_number) + group_by
#
#     # sql_query = (   "SELECT B.THEDATE, B.RACE_NUMBER RACE_NUMBER , A.LEG , B.FK_PARTICIPANTS_ID PARTICIPANTS FROM racecutter.attributes_for_range_leg A "
#     #                 " LEFT JOIN EVENTS B ON B.ID = A.FK_EVENTS_ID "
#     #                 " WHERE B.TIME_START > '" +start_date_str +"' AND  B.TIME_END < '" + end_date_str + "' "  + group_by + " "
#     #                 " ORDER BY THEDATE,RACE_NUMBER, LEG" )
#
#     sql_query = (" SELECT  EV.THEDATE, EV.RACE_NUMBER , ARL.LEG,  EV.FK_PARTICIPANTS_ID PARTICIPANTS FROM attributes_for_range_leg ARL "
#                     "LEFT JOIN EVENTS EV ON EV.ID = ARL.FK_EVENTS_ID "
#                     " INNER JOIN (SELECT THEDATE THEDATE, RACE_NUMBER RACE_NUMBER , FK_EVENTS_ID FROM attributes_for_range_race RR "
#                     " LEFT JOIN EVENTS E ON E.ID = RR.FK_EVENTS_ID "
#                     " WHERE (PORTSIDE = 'AC50_1' AND STBDSIDE = 'AC50_ART') "
#                     " OR (PORTSIDE = 'AC50_ART' AND STBDSIDE = 'AC50_1')) RSET ON (RSET.RACE_NUMBER = EV.RACE_NUMBER AND EV.THEDATE = RSET.THEDATE) "
#                     " GROUP BY EV.RACE_NUMBER "   #,ARL.LEG # EV.THEDATE # , EV.RACE_NUMBER
#                     " ORDER BY EV.THEDATE , EV.RACE_NUMBER , ARL.LEG "
#                     )
#
#     cur= conn.cursor()
#
#     cur.execute(sql_query)
#
#     dat_tables = load_arrays_from_cursor(cur , ret_as_list=False)
#
#     return( dat_tables)
#
# def set_event(conn, event_type_name , start_time , end_time ):
#     if 'datetime64' in str(type(start_time)):  # its a numpy datetime
#         start_time = to_datetime(start_time)
#     start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
#     if 'datetime64' in str(type(end_time)):  # its a numpy datetime
#         end_time = to_datetime(end_time)
#     end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
#     name_query_str = "(SELECT ID FROM EVENT_TYPE WHERE NAME = '{0}') ".format(event_type_name)
#
#     sql_str = (" INSERT INTO EVENTS (START_TIME,END_TIME,EVENT_TYPE_FK_ID) "
#                " VALUES ( '{0}' , '{1}' , {2} )".format(start_time_str, end_time_str,name_query_str ))
#
#     cur = conn.cursor()
#     cur.execute(sql_str)
#     cur.close()
#     pass
#
# def smooth_data(x_arr, y_arr, num_of_points =100  , x_is_datetime = False , y_is_datetime = False):
#     if x_is_datetime:
#         x_arr = mk_ts_from_dt(x_arr)
#     if y_is_datetime:
#         y_arr = mk_ts_from_dt(y_arr)
#
#     new_x = np.linspace(x_arr[0], x_arr[-1], num_of_points)
# #    new_x_dt = mk_dt_from_ts(new_x)
#
#     spl_dat = sp.UnivariateSpline(x_arr, y_arr)
#     # spl_dat.set_smoothing_factor(0.1)
#     return(new_x , spl_dat(new_x))
#
#
# def mk_ts_from_dt(dts):
#     tm_datetime = []
#     for tm in dts:
#         tm_datetime.append( tm.timestamp())
#     return(tm_datetime)
#
# def mk_dt_from_ts(tss):
#     new_x_dt = []
#     for nx in tss:
#         new_x_dt.append( datetime.datetime.fromtimestamp(nx))
#     return(new_x_dt)


