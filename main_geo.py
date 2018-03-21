import json

from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from plotly.graph_objs import *

import DataCruiserLib.data_recovery as dr

from app import app
from pages import  data_plot_page, video_page, data_grid_page

df = dr.select_data('2017-08-22')
sec_step = 1
cursor_loc = 0

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


main_geo_layout = html.Div(
    id='all_main_page',
    className='whole_main',
    children=[
        html.Div(
            id = 'header_bar',
            className = 'header',
            children=[
                html.Div(html.H2(
                    id='title-bar',
                    className='header-left',
                    children='DATA CRUISER'),
                ),
                html.Div(
                    html.H2(
                        id='cursorTime',
                        className='header-middle',
                        children ='22:45:03.3',
                    ),
                ),
                html.Div(
                    id='logo',
                    className = 'header-right',
                     children = html.Img(src  = 'http://127.0.0.1:8000/Test_Logo.png')
                ),
            ]),
        dcc.Interval(
            id='interval-time',
            interval=2000000,  # in milliseconds  default is in stopped mode interval is HUUUUUGE
            n_intervals=0,
            # disabled = False
        ),
        # hidden div to store iloc location in df
        html.Div(className = 'hidden_iloc',
                 id='iloc',
                 children=0,
                 style={'display': 'none'}),

        html.Div(
            className='bar',
            children=[
                html.Table([
                    html.Tr([
                        html.Td(
                            html.Label(
                                children='Pick a Day',
                            ),
                        ),

                        html.Td(
                            dcc.Dropdown(
                                className = 'bar-drop',
                                id = 'day_dropdown',
                                options = dr.get_date_list(),
                                value = '2017-08-22',
                            ),

                        ),

                        html.Td(
                            html.A('Launch New Data Page',
                                href='/pages/data_plot_page',
                                target='_blank',
                                style={'-webkit-appearance': 'button',
                                       'display': 'inline-block'},
                            ),
                        ),
                        html.Td(
                            html.Button('', id='play_button',  className='fa fa-play'
                                        ),

                        ),
                    ]),
                ]),
            ],
        ),

        html.Div(
                className='gutter-map',
                children=[
                    dcc.Dropdown(
                        id='filter1_dropdown',
                        options=[{'label':'test1','value':'test1x'},
                                 {'label':'test2','value':'test2x'},
                                 {'label':'test3','value':'test3x'},
                                 ],

                    ),

            html.Div(className = 'geoplot',
                     children = [
            dcc.Graph(style={'width':'80vh',
                             'height':'80vh',
                             },
                    id='map_test',
                    animate = True,
                ),
            ],),
    ]),

    html.Div(
        className='footer-bar',
        children=[
           html.Div(
               id='data_1',
               className='data-item',
               children = '1234.56'
           ),
            html.Div(
                id='data_2',
                className='data-item',
                children='PLEX'
            ),
            html.Div(
                id='data_3',
                className='data-item',
                children='11:34'
            ),

        ],
    ),

    # PRINT OUT SOMETHING
    # html.Div(className='row', children=[
    #     html.Pre(id='click-data'),
    # ],),

],)

@app.callback(
    Output('map_test','figure'),
    [Input('day_dropdown', 'value'),
     Input('iloc','children')]
)
def update_map_plot(day_string,cursor_loc):
    df = dr.select_data(day_string)
    return{
        'data': Data([
            Scattermapbox(
                name='Track',
                lat=dr.get_lats(df),
                lon=dr.get_lons(df),
                text=dr.get_time(df),
                mode='lines+markers',
                marker=Marker(
                    size=2
                ),
                line=Line(
                    color='black'
                ),
            ),
            Scattermapbox(
                name='CursorPos',
                lat=dr.get_lats(df.iloc[cursor_loc]),  # replace with cursor iloc
                lon=dr.get_lons(df.iloc[cursor_loc]),
                text=['WOXI_cursor'],
                mode='markers+text',
                marker=Marker(
                    size=20,
                    color='red'
                ),
            ),

        ]),


        'layout': Layout(
            # autosize=True,
            hovermode='closest',
            mapbox=dict(
                accesstoken='pk.eyJ1IjoiZnJlc2h3dXpoZXJlIiwiYSI6ImNqYTFsZm91ZzBmcG8ycWxmMnR4azVxZ2MifQ.KivcUMioVWwbYkwIw8xFgg',
                bearing=0,
                # center=dict(
                #     lat=dr.get_lat_center(df),
                #     lon=dr.get_lon_center(df)
                # ),
                pitch=0,
                # zoom=10
            ),
        )
    }

# Update the index
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/pages/data_plot_page':
        return data_plot_page.data_plot_page_layout
    elif pathname == '/pages/data_grid_page':
        return data_grid_page.layout
    elif pathname == '/pages/video_page':
        return video_page.layout
    else:
        return main_geo_layout
    # You could also return a 404 "URL not found" page here


# collect clicked data
@app.callback(
    Output('click-data', 'children'),
    [Input('map_test', 'clickData')]
)
def display_click_data(clickData):
    ###
    # This is the click on the map that returns the data that was clicked on.
    # need to set tthe iloc with the data from the call back (it is present in the json
    # TODO format time and present to proper place
    # TODO update graphs when time changes
    return json.dumps(clickData, indent=2)

@app.callback(Output('interval-time','interval'),
             [Input('play_button','className')])
def timestepper(c_name):
    print(str(c_name))
    if c_name == 'fa fa-play':
        ot = 1000
    else:
        ot = 2000000000
    return(ot)



@app.callback(Output('play_button','className'),
              [Input('play_button','n_clicks')],
              [State('play_button','className')]
              )
def update_run_state(num_of_clicks, but_state):
    # print('My-Play prior -- ' + str(my_play))
    if but_state == 'fa fa-stop':
        my_play = True
        ret = 'fa fa-play'
    else:
        my_play = False
        ret = 'fa fa-stop'
    print('My-Play post -- ' + str(my_play))
    data_dic = {'play_state':my_play}
    dr.write_state_to_file('test_storage_file' , data_dic)
    return(ret)


@app.callback(Output('iloc', 'children'),
              [Input('interval-time','n_intervals')],
              [State('iloc','children')]
            )
def update_clock(time_count , cursor_loc):
    # nincrement cursor loc
    cursor_loc = cursor_loc + 1
    data_dic = {'time_cursor': cursor_loc}
    dr.write_state_to_file('test_time_file', data_dic)
    return(cursor_loc)

    # cursor_date = df['DATETIME'][cursor_loc]
    # cursor_time = cursor_date.strftime('%H:%M:%S.%f')[:-5]
    # data_dic = {'time_cursor': cursor_time}
    # dr.write_state_to_file('test_time_file', data_dic)


@app.callback(Output('cursorTime','children'),
              [Input('iloc', 'children')],
              )
def update_time_cursor(cursor_loc):
    cursor_date = df['DATETIME'][cursor_loc]
    cursor_time = cursor_date.strftime('%H:%M:%S.%f')[:-5]
    return(cursor_time)


@app.callback(Output('data_1','children'),
              [Input('iloc', 'children')],
              )
def update_data_1(i):
    data_val = df.iloc[i]['BSP']
    # data_val = 6
    return(data_val)

@app.callback(Output('data_2','children'),
              [Input('iloc', 'children')],
              )
def update_data_2(i):
    data_val = df.iloc[i]['TWA']
    # data_val = 6
    return(data_val)

@app.callback(Output('data_3','children'),
              [Input('iloc', 'children')],
              )
def update_data_3(i):
    data_val = df.iloc[i]['VMG']
    # data_val = 6
    return(data_val)



# app.css.append_css({
#     'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

app.css.append_css({
    'external_url': 'http://127.0.0.1:8000/DataCruiser.css'})

app.css.append_css({
    'external_url': 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css'})


if __name__ == '__main__':
    app.run_server(debug=True)
