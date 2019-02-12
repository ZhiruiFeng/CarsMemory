# -*- coding: utf-8 -*-
import json
import math

import pandas as pd
import flask
import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.plotly as py
from plotly import graph_objs as go
import datetime


from app import app, indicator, millify, df_to_table

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]



# returns choropleth map figure based on status filter
def choropleth_map(status, df):
    # TODO add selection to get the time scope for display

    df = df.groupby("location").count()

    scl = [[0.0, "rgb(38, 78, 134)"], [1.0, "#0091D5"]] # colors scale

    data = [
        dict(
            type="choropleth",
            colorscale=scl,
            locations=df.index,
            z=df["dashcam_id"],
            locationmode="USA-states",
            marker=dict(line=dict(color="rgb(255,255,255)", width=2)),
        )
    ]

    layout = dict(
        geo=dict(
            scope="usa",
            projection=dict(type="albers usa"),
            lakecolor="rgb(255, 255, 255)",
        ),
        margin=dict(l=10, r=10, t=0, b=0),
    )
    return dict(data=data, layout=layout)


# returns pie chart that shows lead source repartition
def lead_source(status, df):
    # TODO add selection to get the time scope for display

    nb_total = df['total'].sum()
    types = ['airfield', 'alley', 'bridge', 'crosswalk', 'downtown',
             'gas', 'harbor', 'highway', 'neighborhood', 'park',
             'parking', 'ruin', 'snowfield', 'station', 'street',
             'wild', "village"]
    values = []

    # compute % for each leadsource type
    for scene_type in types:
        nb_type = df[scene_type].sum()
        values.append(nb_type / nb_total * 100)

    trace = go.Pie(
        labels=types,
        values=values,
        marker={"colors": ["#264e86", "#0074e4", "#74dbef", "#eff0f4"]},
    )

    layout = dict(margin=dict(l=15, r=10, t=0, b=65), legend=dict(orientation="h"))
    return dict(data=[trace], layout=layout)


def converted_leads_count(period, df):
    df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], format="%Y-%m-%d")
    df = df[df["Status"] == "Closed - Converted"]

    df = (
        df.groupby([pd.Grouper(key="CreatedDate", freq=period)])
        .count()
        .reset_index()
        .sort_values("CreatedDate")
    )

    trace = go.Scatter(
        x=df["CreatedDate"],
        y=df["Id"],
        name="converted leads",
        fill="tozeroy",
        fillcolor="#e6f2ff",
    )

    data = [trace]

    layout = go.Layout(
        xaxis=dict(showgrid=False),
        margin=dict(l=33, r=25, b=37, t=5, pad=4),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    return {"data": data, "layout": layout}

layout = [

    # top controls
    html.Div(
        [
            html.Div(
                dcc.Dropdown(
                    id="converted_leads_dropdown",
                    options=[
                        {"label": "By day", "value": "D"},
                        {"label": "By week", "value": "W-MON"},
                        {"label": "By month", "value": "M"},
                    ],
                    value="D",
                    clearable=False,
                ),
                className="two columns",
            ),
            html.Div(
                dcc.Dropdown(
                    id="lead_source_dropdown",
                    options=[
                        {"label": "All status", "value": "all"},
                        {"label": "Open leads", "value": "open"},
                        {"label": "Converted leads", "value": "converted"},
                        {"label": "Lost leads", "value": "lost"},
                    ],
                    value="all",
                    clearable=False,
                ),
                className="two columns",
            ),
        ],
        className="row",
        style={"marginBottom": "10"},
    ),

    # indicators row div
    html.Div(
        [
            indicator(
                "#00cc96",
                "Unique States Count",
                "left_leads_indicator"
            ),
            indicator(
                "#119DFF",
                "Connected DashCam Count",
                "middle_leads_indicator"
            ),
            indicator(
                "#EF553B",
                "Recording Dates Count",
                "right_leads_indicator",
            ),
        ],
        className="row",
    ),

    # charts row div
    html.Div(
        [
            html.Div(
                [
                    html.P("DashCam count per state"),
                    dcc.Graph(
                        id="map",
                        style={"height": "90%", "width": "98%"},
                        config=dict(displayModeBar=False),
                    ),
                ],
                className="six columns chart_div"
            ),

            html.Div(
                [
                    html.P("Scene feature distribution"),
                    dcc.Graph(
                        id="lead_source",
                        style={"height": "90%", "width": "98%"},
                        config=dict(displayModeBar=False),
                    ),
                ],
                className="six columns chart_div"
            ),
        ],
        className="row",
        style={"marginTop": "5"},
    ),

    html.Div(
        [
            html.P("Recent week's keyframes percentage for each user"),
            dcc.Graph(id='graph-with-slider'),
            dcc.Slider(
                id='date-slider',
                min=0,
                max=7,
                value=7,
                marks={(7-N): (datetime.date.today()- datetime.timedelta(days=N)).strftime("%Y%m%d") for N in range(7, -1, -1)}
            )
        ],
        className="row"
    ),
]


# updates left indicator based on df updates
@app.callback(
    Output("left_leads_indicator", "children"), [Input("leads_df", "children")]
)
def left_leads_indicator_callback(df):
    df = pd.read_json(df, orient="split")
    unique_location = len(df['location'].unique())
    return unique_location


# updates middle indicator based on df updates
@app.callback(
    Output("middle_leads_indicator", "children"), [Input("leads_df", "children")]
)
def middle_leads_indicator_callback(df):
    df = pd.read_json(df, orient="split")
    unique_cam = len(df['dashcam_id'].unique())
    return unique_cam


# updates right indicator based on df updates
@app.callback(
    Output("right_leads_indicator", "children"), [Input("leads_df", "children")]
)
def right_leads_indicator_callback(df):
    df = pd.read_json(df, orient="split")
    unique_date = len(df['store_date'].unique())
    return unique_date


# update pie chart figure based on dropdown's value and df updates
@app.callback(
    Output("lead_source", "figure"),
    [Input("lead_source_dropdown", "value"), Input("leads_df", "children")],
)
def lead_source_callback(status, df):
    df = pd.read_json(df, orient="split")
    return lead_source(status, df)


# update heat map figure based on dropdown's value and df updates
@app.callback(
    Output("map", "figure"),
    [Input("lead_source_dropdown", "value"), Input("leads_df", "children")],
)
def map_callback(status, df):
    df = pd.read_json(df, orient="split")
    return choropleth_map(status, df)


# Update the figure with slider
@app.callback(
    Output('graph-with-slider', 'figure'),
    [Input('date-slider', 'value'), Input("leads_df", "children")],
)
def update_figure(selected_date, df):
    df = pd.read_json(df, orient="split")
    selected_date = 7 - selected_date
    selected_day = (datetime.date.today()- datetime.timedelta(days=selected_date)).strftime("%Y%m%d")
    filtered_df = df[df.store_date == int(selected_day)]
    traces = []
    for i in filtered_df.location.unique():
        df_by_continent = filtered_df[filtered_df['location'] == i]
        traces.append(go.Scatter(
            x=df_by_continent['frames'],
            y=df_by_continent['keyframes'],
            text=df_by_continent['dashcam_id'],
            mode='markers',
            opacity=0.7,
            marker={
                'size': 15,
                'line': {'width': 0.5, 'color': 'white'}
            },
            name=i
        ))

    return {
        'data': traces,
        'layout': go.Layout(
            xaxis={'type': 'log', 'title': 'Total Frames'},
            yaxis={'title': 'Keyframes Count', 'range': [20, 90]},
            margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
            legend={'x': 0, 'y': 1},
            hovermode='closest'
        )
    }

# update table based on dropdown's value and df updates
@app.callback(
    Output("leads_table", "children"),
    [Input("lead_source_dropdown", "value"), Input("leads_df", "children")],
)
def leads_table_callback(status, df):
    df = pd.read_json(df, orient="split")
    if status == "open":
        df = df[
            (df["Status"] == "Open - Not Contacted")
            | (df["Status"] == "Working - Contacted")
        ]
    elif status == "converted":
        df = df[df["Status"] == "Closed - Converted"]
    elif status == "lost":
        df = df[df["Status"] == "Closed - Not Converted"]
    df = df[["CreatedDate", "Status", "Company", "State", "LeadSource"]]
    return df_to_table(df)


# update pie chart figure based on dropdown's value and df updates
@app.callback(
    Output("converted_leads", "figure"),
    [Input("converted_leads_dropdown", "value"), Input("leads_df", "children")],
)
def converted_leads_callback(period, df):
    df = pd.read_json(df, orient="split")
    return converted_leads_count(period, df)
