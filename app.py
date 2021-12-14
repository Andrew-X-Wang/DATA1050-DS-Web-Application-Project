import dash
from dash import dcc
from dash import html
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from psycopg2 import OperationalError

from database import *


## Determining if feature is continuous
THRESH = 0.01
def is_cont(data, cat_name):
    if data[cat_name].dtype != 'float64':
        return False
    if data[cat_name].nunique() / data[cat_name].count() < THRESH:
        return False
    return True
    

# Definitions of constants. This projects uses extra CSS stylesheet at `./assets/style.css`
COLORS = ['rgb(67,67,67)', 'rgb(115,115,115)', 'rgb(49,130,189)', 'rgb(189,189,189)']
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', '/assets/style.css']

# Define the dash app first
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


def page_title():
     return html.Div(children=[
         html.H1("Covid-19 Dashboard")
     ])


def page_description():
    return html.Div(children=[
        html.H3("Team Members:"),
        html.H5("Mark Adut, Aryaman Dutta, Emre Toner, Andrew Wang"),
        html.H2("Project Description:"),
        html.H5(
            "In this project, we used Heroku to store a relational database of COVID-19 \
            data sourced from the Center for Systems Science and Engineering (CSSE) at \
            Johns Hopkins University (JHU). The data was acquired in raw form from the JHU \
            github, from which a CSV was sourced and subsequently interfaced with pyscopg2 \
            to create and store the data in an SQL relation. This data is continually updated \
            on a daily basis to reflect the current state of the covid-19 pandemic across the \
            world. There were two separate data frames that were sourced from JHU. The first \
            dataframe was the historical dataframe, which contained information on the covid \
            metrics since the beginning of the pandemic but not including the latest update. \
            The second data frame was the updated-daily ( incremental update every 24 hrs) \
            data frame which contained raw data on the daily updates of covid metrics. "
        )
    ])


# Define component functions
def target_vis():
    global CONN
    df_cov = get_covid(CONN, 'covid')

    return html.Div(children=[
        html.Div(children=[
            html.H2(children='Target Variable Visualization'),
            dcc.Dropdown(
                id='regressor_feature_dd',
                options=[{'label': col, 'value': col} for col in df_cov.columns],
                multi=False,
                placeholder='Feature to Plot Over',
                value=df_cov.columns[0]
            ),
            html.Div(children=[
                dcc.Graph(id='target_var_fig')]),
        ])
    ], className='row')


def timeline_vis():
    global CONN
    df_hist = get_covid(CONN, 'covidhistorical')
    hist_feats = df_hist.columns

    return html.Div(children=[
        html.Div(children=[
            html.H2("Timeline"),
            dcc.Dropdown(
                id='hist_feature_dd',
                options=[{'label': f, 'value': f} for f in hist_feats if df_hist[f].dtype != 'object'],
                multi=False,
                placeholder='Historical Feature to Visualize',
                value='new_cases_smoothed'
            ),
            # @TODO: remove 'date' as a label/value option
            dcc.Dropdown(
                id='hist_filter_feat_dd',
                options=[{'label': f, 'value': f} for f in hist_feats if df_hist[f].dtype == 'object'],
                multi=False,
                placeholder='Feature to Filter',
                value='location'
            ),
            dcc.Dropdown(
                id='hist_filter_val_dd',
                options=[],
                multi=False,
                placeholder='Value to Filter By',
                value=df_hist.iloc[0]['location'] # Doesn't do anything, pretty sure. Since returned from callback immediately
            ),
            html.Div(children=[
                dcc.Graph(id='hist_timeline_fig')])
            ])
    ])


def history_compare():
    global CONN
    df_hist = get_covid(CONN, 'covidhistorical')
    hist_feats = df_hist.columns

    return html.Div(children=[
        html.Div(children=[
            html.H2("Historical Comparison"),
            dcc.Dropdown(
                id='locations_to_compare_dd',
                options=[{'label': d, 'value': d} for d in df_hist['location'].unique()],
                multi=True,
                placeholder='Locations to Compare',
                value=[df_hist.iloc[0]['location']]
            ),
            dcc.Dropdown(
                id='dates_to_compare_dd',
                options=[{'label': d, 'value': d} for d in df_hist['date'].unique()],
                multi=True,
                placeholder='Dates to Compare To',
                value=[df_hist['date'].min()] # put in db_info?
            ),
            dcc.Dropdown(
                id='feats_to_compare_dd',
                options=[{'label': f, 'value': f} for f in hist_feats],
                multi=True,
                placeholder='Features to Compare',
                value=['new_tests_smoothed', 'new_cases']
            ),
            html.Div(children=[
                dcc.Graph(id='hist_comparison_fig')
            ])
        ])
    ])


# Sequentially add page components to the app's layout
def dynamic_layout():
    global CONN
    CONN = create_connection(
        "dcegl8mv856qb8", "ndvqpnrwxtmwvu", "eec515b7f7a6c5c44d4df10499aa344d698310c1b39474bd2aefca27633fb241", "ec2-3-89-214-80.compute-1.amazonaws.com", "5432"
    )

    # The HTML Layout
    return html.Div([
        page_title(),
        page_description(),
        timeline_vis(),
        target_vis(),
        history_compare(),
    ], className='row', id='content')


# set layout to a function which updates upon reloading

app.layout = dynamic_layout


# Defines the dependencies of interactive components

# Updating Target Variable (new_cases) Visualization for Latest Data
@app.callback(
    dash.dependencies.Output('target_var_fig', 'figure'),
    dash.dependencies.Input('regressor_feature_dd', 'value')
)
def update_target_visualization(feature_name):
    global CONN
    df_cov = get_covid(CONN, 'covid')

    # if feature_name != None:
    target_var = 'new_cases_smoothed'
    fig = None
    if feature_name != target_var:
        if is_cont(df_cov, feature_name):
            fig = px.scatter(df_cov, x=feature_name, y=target_var, 
                             title=f"Scatter {target_var} over {feature_name}")
        else:
            fig = px.bar(df_cov, x = feature_name, y= target_var,
                         title=f"BoxPlot {target_var} over {feature_name}")

    fig.update_layout(template='plotly_dark', title='Visualizing Target Variable for Latest Data',
                          plot_bgcolor='#23272c', paper_bgcolor='#23272c')
    return fig


# Updating Historical Data Visualization
@app.callback(
    [dash.dependencies.Output('hist_filter_val_dd', 'options'),
     dash.dependencies.Output('hist_filter_val_dd', 'value')],
    dash.dependencies.Input('hist_filter_feat_dd', 'value')
)
def update_filter_val_options(filter_feat):
    global CONN    
    df_hist = get_covid(CONN, 'covidhistorical')

    not_null_mask = df_hist[filter_feat].notnull()
    unique_vals = df_hist[filter_feat][not_null_mask].unique()
    options = [{'label': val, 'value': val} for val in unique_vals]
    value = options[0]['value']
    return options, value


@app.callback(
    dash.dependencies.Output('hist_timeline_fig', 'figure'),
    [dash.dependencies.Input('hist_feature_dd', 'value'),
     dash.dependencies.Input('hist_filter_feat_dd', 'value'),
     dash.dependencies.Input('hist_filter_val_dd', 'value')]
)
def update_timeline_vis(plot_feature, filter_feature, filter_value):
    global CONN
    df_hist = get_covid(CONN, 'covidhistorical')

    hist_time_feature = 'date' # can put in db_info
    hist_filter_mask = df_hist[filter_feature] == filter_value
    df_hist_filtered = df_hist[hist_filter_mask]
    
    fig = None
    fig=px.line(df_hist_filtered, x=hist_time_feature, y=plot_feature, color='location')

    fig.update_layout(template='plotly_dark', title=f'Historical Timeline of {plot_feature} Over {filter_feature}',
                          plot_bgcolor='#23272c', paper_bgcolor='#23272c')
    return fig


# @TODO: some features don't work well here, due to concatenation bug: either remove features, or fix bug (ideally the former)
# Updating Historical Comparison Visualization
@app.callback(
    dash.dependencies.Output('hist_comparison_fig', 'figure'),
    [dash.dependencies.Input('locations_to_compare_dd', 'value'),
     dash.dependencies.Input('dates_to_compare_dd', 'value'),
     dash.dependencies.Input('feats_to_compare_dd', 'value')]
)
def update_history_compare_vis(locations, hist_dates, features):
    global CONN
    df_cov = get_covid(CONN, 'covid')
    df_hist = get_covid(CONN, 'covidhistorical')
    
    # Date Mask
    dummy_date = '0000-01-01'
    hist_date_mask = df_hist['date'] == dummy_date # Should be all False
    for hist_date in hist_dates:
        curr_date_mask = df_hist['date'] == hist_date
        hist_date_mask = [has_date_c or has_date_h for has_date_c, has_date_h in zip(curr_date_mask, hist_date_mask)]

    df_hist_date = df_hist[hist_date_mask]

    # Location Mask
    dummy_loc = 'Roshar'
    hist_loc_mask = df_hist_date['location'] == dummy_loc
    for loc in locations:
        curr_loc_mask = df_hist_date['location'] == loc
        hist_loc_mask = [has_loc_c or has_loc_h for has_loc_c, has_loc_h in zip(curr_loc_mask, hist_loc_mask)]

    # Combined Filtered DF
    df_hist_filtered = df_hist_date[hist_loc_mask]

    # Concatenating historical and latest DF
    df_cov_new = df_cov.rename(columns = {'last_updated_date':'date'})
    df_compare = pd.concat([df_cov_new, df_hist_filtered], sort=False).reset_index()

    feats_to_plot = ['date', 'location'] + features
    df_compare = df_compare[feats_to_plot]

    # Setting up DF to plot, 'grouped stacked' bar plot
    df_total = pd.DataFrame(columns=feats_to_plot)
    df_total['feature'] = None
    df_total['feature_val'] = None
    for f in features:
        feats_to_remove = set(features).difference(set([f]))
        # Rename f to 'feature_val', drop rest of features from DF
        df_new = df_compare.rename(columns={f:'feature_val'}).drop(feats_to_remove, axis=1)
        df_new['feature'] = f
        df_total = pd.concat([df_total, df_new])
    
    print(df_total.shape)

    # Creating figure
    fig = go.Figure()
    # Color setup
    rgb=[55, 83, 109]
    signs = np.random.randint(3, size=(df_total.shape[0], 3))

    # By location (@TODO: make location a dropdown option as well)
    for i, loc in enumerate(locations):
        df_row = df_total[df_total['location']==loc]
        print("DF ROW")
        print(df_row)
        rgb_i = [(c + signs[i, j] * i * 30)%256 for j, c in enumerate(rgb)]
        # if df_total.iloc[i]['date'] >= '2021-12-10' or df_total.iloc[i]['date'] == '2020-06-03': # @TODO: change latest date
        fig.add_trace(go.Bar(x=[df_row.feature, df_row.date],
                        y = df_row.feature_val,
                        marker_color=f'rgb({rgb_i[0]}, {rgb_i[1]}, {rgb_i[2]})', name=loc
                    ))

    fig.update_layout(
        barmode='stack',
        bargap=0.15, # gap between bars of adjacent location coordinates.
        bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    fig.update_layout(template='plotly_dark', title=f'Historical Comparison by Feature and Location',
                          plot_bgcolor='#23272c', paper_bgcolor='#23272c')

    return fig

CONN = None
if __name__ == '__main__':
    app.run_server(debug=True, port=1050, host='0.0.0.0')
