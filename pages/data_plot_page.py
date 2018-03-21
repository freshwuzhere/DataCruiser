from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd

import DataCruiserLib.data_recovery as dr

from app import app

df = pd.read_csv('dataframe')

data_plot_page_layout = html.Div(children=[
    html.H2(children='DATA CRUISER'
            ),
    html.H1(
        id='cursorTime',
        className='cursorTime',
        # style=''
    ),


    dcc.Graph(
        id='data plot',
        figure={
            'data': [
                {'x': dr.get_time(df), 'y': dr.get_bsp(df), 'type': 'scatter', 'name': 'BSP'},
                {'x': dr.get_time(df), 'y': dr.get_vmg(df), 'type': 'scatter', 'name': u'VMG'},
            ],
            'layout': {
                'title': 'BSP and VMG vs Time',
                'height': '900',
                'width': '1400',
            },
        },
    ),

])