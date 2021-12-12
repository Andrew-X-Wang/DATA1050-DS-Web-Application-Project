import dash
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from database import fetch_all_bpa_as_df
from matplotlib import pyplot as plt
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
import pandas.io.sql as sqlio

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


#heroku connection
connection = create_connection(
    "dcegl8mv856qb8", "ndvqpnrwxtmwvu", "eec515b7f7a6c5c44d4df10499aa344d698310c1b39474bd2aefca27633fb241", "ec2-3-89-214-80.compute-1.amazonaws.com", "5432"
)

#show all tables in db 
cursor = connection.cursor()
cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
cursor.fetchall()

# ------------ Latest Covid Data --------------

# extract raw data from Jonh's Hopkins Repo
df_covid = pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv')

# specify table name
table_name = "covid"

try:
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    create_table_query = f"CREATE TABLE {table_name} ("
    create_table_query += ", ".join([col + (" double precision" if df_covid.dtypes[i] == 'float' else " varchar(60)") for i, col in enumerate(df_covid.columns)]) + ");"
    cursor.execute(create_table_query)

except Exception as e:
    print(e)
    
for i in range(df_covid.shape[0]):
    row_list = df_covid.iloc[i].tolist()
    str_row = [str(f) for f in row_list]
    str_row = ["NULL" if s == 'nan' else s for s in str_row]
    insert_p1 = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for i in df_covid.columns])})"
    try:
#         cursor.execute(insert_query)
        cursor.execute(insert_p1, df_covid.iloc[i].tolist())
       
    except Exception as e:
        print(e)
    

#convert sql data into pandas df
sql = "SELECT * from covid;"
df_cov = sqlio.read_sql_query(sql, connection)
print(df_cov.head(10))



# ------------ Historical Covid Data --------------

# historical table data from git
covid_historical_df = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv')

#new table name
table_name = "CovidHistorical"

try:
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    create_table_query = f"CREATE TABLE {table_name} ("
    create_table_query += ", ".join([col + (" double precision" if covid_historical_df.dtypes[i] == 'float' else " varchar(60)") for i, col in enumerate(covid_historical_df.columns)]) + ");"
    cursor.execute(create_table_query)

except Exception as e:
    print(e)
    
for i in range(covid_historical_df.shape[0]):
    row_list = covid_historical_df.iloc[i].tolist()
    str_row = [str(f) for f in row_list]
    str_row = ["NULL" if s == 'nan' else s for s in str_row]
    insert_p1 = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for i in covid_historical_df.columns])})"
    try:
#         cursor.execute(insert_query)
        cursor.execute(insert_p1, covid_historical_df.iloc[i].tolist())
       
    except Exception as e:
        print(e)
    
sql = "SELECT * from CovidHistorical;"
historical_covid_df = sqlio.read_sql_query(sql, connection)
# print(historical_covid_df.head(10))


## READ COVID DATA FROM OWID REPO
data_url = 'https://github.com/owid/covid-19-data/raw/master/public/data/latest/owid-covid-latest.csv'
df_cov = pd.read_csv(data_url)

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


# Define component functions
def page_header():
    """
    Returns the page header as a dash `html.Div`
    """
    return html.Div(id='header', children=[
        html.Div([html.H3('Visualization with datashader and Plotly')],
                 className="ten columns"),
        html.A([html.Img(id='logo', src=app.get_asset_url('github.png'),
                         style={'height': '35px', 'paddingTop': '7%'}),
                html.Span('Blownhither', style={'fontSize': '2rem', 'height': '35px', 'bottom': 0,
                                                'paddingLeft': '4px', 'color': '#a3a7b0',
                                                'textDecoration': 'none'})],
               className="two columns row",
               href='https://github.com/blownhither/'),
    ], className="row")


def description():
    """
    Returns overall project description in markdown
    """
    return html.Div(children=[dcc.Markdown('''
        # Energy Planner
        As of today, 138 cities in the U.S. have formally announced 100% renewable energy goals or
        targets, while others are actively considering similar goals. Despite ambition and progress,
        conversion towards renewable energy remains challenging.

        Wind and solar power are becoming more cost effective, but they will always be unreliable
        and intermittent sources of energy. They follow weather patterns with potential for lots of
        variability. Solar power starts to die away right at sunset, when one of the two daily peaks
        arrives (see orange curve for load).

        **Energy Planner is a "What-If" tool to assist making power conversion plans.**
        It can be used to explore load satisfiability under different power contribution with 
        near-real-time energy production & consumption data.

        ### Data Source
        Energy Planner utilizes near-real-time energy production & consumption data from [BPA 
        Balancing Authority](https://www.bpa.gov/news/AboutUs/Pages/default.aspx).
        The [data source](https://transmission.bpa.gov/business/operations/Wind/baltwg.aspx) 
        **updates every 5 minutes**. 
        ''', className='eleven columns', style={'paddingLeft': '5%'})], className="row")


def static_stacked_trend_graph(stack=False):
    """
    Returns scatter line plot of all power sources and power load.
    If `stack` is `True`, the 4 power sources are stacked together to show the overall power
    production.
    """
    df = fetch_all_bpa_as_df()
    if df is None:
        return go.Figure()
    sources = ['Wind', 'Hydro', 'Fossil/Biomass', 'Nuclear']
    x = df['Datetime']
    fig = go.Figure()
    for i, s in enumerate(sources):
        fig.add_trace(go.Scatter(x=x, y=df[s], mode='lines', name=s,
                                 line={'width': 2, 'color': COLORS[i]},
                                 stackgroup='stack' if stack else None))
    fig.add_trace(go.Scatter(x=x, y=df['Load'], mode='lines', name='Load',
                             line={'width': 2, 'color': 'orange'}))
    title = 'Energy Production & Consumption under BPA Balancing Authority'
    if stack:
        title += ' [Stacked]'
    fig.update_layout(template='plotly_dark',
                      title=title,
                      plot_bgcolor='#23272c',
                      paper_bgcolor='#23272c',
                      yaxis_title='MW',
                      xaxis_title='Date/Time')
    return fig


def what_if_description():
    """
    Returns description of "What-If" - the interactive component
    """
    return html.Div(children=[
        dcc.Markdown('''
        # " What If "
        So far, BPA has been relying on hydro power to balance the demand and supply of power. 
        Could our city survive an outage of hydro power and use up-scaled wind power as an
        alternative? Find below **what would happen with 2.5x wind power and no hydro power at 
        all**.   
        Feel free to try out more combinations with the sliders. For the clarity of demo code,
        only two sliders are included here. A fully-functioning What-If tool should support
        playing with other interesting aspects of the problem (e.g. instability of load).
        ''', className='eleven columns', style={'paddingLeft': '5%'})
    ], className="row")


def what_if_tool():
    """
    Returns the What-If tool as a dash `html.Div`. The view is a 8:3 division between
    demand-supply plot and rescale sliders.
    """
    return html.Div(children=[
        html.Div(children=[dcc.Graph(id='what-if-figure')], className='nine columns'),

        html.Div(children=[
            html.H5("Rescale Power Supply", style={'marginTop': '2rem'}),
            html.Div(children=[
                dcc.Slider(id='wind-scale-slider', min=0, max=4, step=0.1, value=2.5, className='row',
                           marks={x: str(x) for x in np.arange(0, 4.1, 1)})
            ], style={'marginTop': '5rem'}),

            html.Div(id='wind-scale-text', style={'marginTop': '1rem'}),

            html.Div(children=[
                dcc.Slider(id='hydro-scale-slider', min=0, max=4, step=0.1, value=0,
                           className='row', marks={x: str(x) for x in np.arange(0, 4.1, 1)})
            ], style={'marginTop': '3rem'}),
            html.Div(id='hydro-scale-text', style={'marginTop': '1rem'}),
        ], className='three columns', style={'marginLeft': 5, 'marginTop': '10%'}),
    ], className='row eleven columns')


def architecture_summary():
    """
    Returns the text and image of architecture summary of the project.
    """
    return html.Div(children=[
        dcc.Markdown('''
            # Project Architecture
            This project uses MongoDB as the database. All data acquired are stored in raw form to the
            database (with de-duplication). An abstract layer is built in `database.py` so all queries
            can be done via function call. For a more complicated app, the layer will also be
            responsible for schema consistency. A `plot.ly` & `dash` app is serving this web page
            through. Actions on responsive components on the page is redirected to `app.py` which will
            then update certain components on the page.  
        ''', className='row eleven columns', style={'paddingLeft': '5%'}),

        html.Div(children=[
            html.Img(src="https://docs.google.com/drawings/d/e/2PACX-1vQNerIIsLZU2zMdRhIl3ZZkDMIt7jhE_fjZ6ZxhnJ9bKe1emPcjI92lT5L7aZRYVhJgPZ7EURN0AqRh/pub?w=670&amp;h=457",
                     className='row'),
        ], className='row', style={'textAlign': 'center'}),

        dcc.Markdown('''
        
        ''')
    ], className='row')


def target_vis():
    return html.Div(children=[
        html.Div(children=[
            html.H2(children='Target Variable Visualization'),
            dcc.Dropdown(
                id='regressor_feature',
                options=[{'label': col, 'value': col} for col in df_cov.columns],
                multi=False,
                placeholder='Feature to Plot Over',
                value=df_cov.columns[0]
            ),
            html.Div(children=[
                dcc.Graph(id='target_var_fig')]),
        ])
    ], className='row')


# Sequentially add page components to the app's layout
def dynamic_layout():
    return html.Div([
        target_vis(),
        # page_header(),
        # html.Hr(),
        # description(),
        # dcc.Graph(id='trend-graph', figure=static_stacked_trend_graph(stack=False)),
        # dcc.Graph(id='stacked-trend-graph', figure=static_stacked_trend_graph(stack=True)),
        # what_if_description(),
        # what_if_tool(),
        # architecture_summary(),
    ], className='row', id='content')


# set layout to a function which updates upon reloading
app.layout = dynamic_layout


# Defines the dependencies of interactive components

@app.callback(
    dash.dependencies.Output('target_var_fig', 'figure'),
    dash.dependencies.Input('regressor_feature', 'value')
)
def update_target_visualization(feature_name):
    # if feature_name != None:
    target_var = 'new_cases'
    fig = None
    if feature_name != target_var:
        if is_cont(df_cov, feature_name):
            fig = px.scatter(df_cov, x=feature_name, y=target_var, 
                             title=f"Scatter {target_var} over {feature_name}")
        else:
            fig = px.bar(df_cov, x = feature_name, y= target_var,
                         title=f"BoxPlot {target_var} over {feature_name}")

    fig.update_layout(template='plotly_dark', title='Supply/Demand after Power Scaling',
                          plot_bgcolor='#23272c', paper_bgcolor='#23272c', yaxis_title='MW',
                          xaxis_title='Date/Time')
    return fig
    

# @app.callback(
#     dash.dependencies.Output('wind-scale-text', 'children'),
#     [dash.dependencies.Input('wind-scale-slider', 'value')])
# def update_wind_sacle_text(value):
#     """Changes the display text of the wind slider"""
#     return "Wind Power Scale {:.2f}x".format(value)


# @app.callback(
#     dash.dependencies.Output('hydro-scale-text', 'children'),
#     [dash.dependencies.Input('hydro-scale-slider', 'value')])
# def update_hydro_sacle_text(value):
#     """Changes the display text of the hydro slider"""
#     return "Hydro Power Scale {:.2f}x".format(value)



# @app.callback(
#     dash.dependencies.Output('what-if-figure', 'figure'),
#     [dash.dependencies.Input('wind-scale-slider', 'value'),
#      dash.dependencies.Input('hydro-scale-slider', 'value')])
# def what_if_handler(wind, hydro):
#     """Changes the display graph of supply-demand"""
#     df = fetch_all_bpa_as_df(allow_cached=True)
#     x = df['Datetime']
#     supply = df['Wind'] * wind + df['Hydro'] * hydro + df['Fossil/Biomass'] + df['Nuclear']
#     load = df['Load']

#     fig = go.Figure()
#     fig.add_trace(go.Scatter(x=x, y=supply, mode='none', name='supply', line={'width': 2, 'color': 'pink'},
#                   fill='tozeroy'))
#     fig.add_trace(go.Scatter(x=x, y=load, mode='none', name='demand', line={'width': 2, 'color': 'orange'},
#                   fill='tonexty'))
#     fig.update_layout(template='plotly_dark', title='Supply/Demand after Power Scaling',
#                       plot_bgcolor='#23272c', paper_bgcolor='#23272c', yaxis_title='MW',
#                       xaxis_title='Date/Time')
#     return fig


if __name__ == '__main__':
    app.run_server(debug=True, port=1051, host='0.0.0.0')

