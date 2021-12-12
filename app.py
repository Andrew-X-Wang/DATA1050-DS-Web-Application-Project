import dash
from dash import dcc
from dash import html
import numpy as np
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px

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
latest_feats = df_cov.columns

df_hist = pd.read_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv')
hist_feats = df_hist.columns

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


def timeline_vis():
    return html.Div(children=[
        html.Div(children=[
            html.H2("Timeline"),
            dcc.Dropdown(
                id='hist_feature_dd',
                options=[{'label': f, 'value': f} for f in hist_feats if df_hist[f].dtype != 'object'],
                multi=False,
                placeholder='Historical Feature to Visualize',
                value='new_cases'
            ),
            # @TODO: remove 'date' as a label/value option
            dcc.Dropdown(
                id='hist_agg_feat_dd',
                options=[{'label': f, 'value': f} for f in hist_feats if df_hist[f].dtype == 'object'],
                multi=False,
                placeholder='Aggregation Feature',
                value='location'
            ),
            dcc.Dropdown(
                id='hist_agg_val_dd',
                options=[],
                multi=False,
                placeholder='Aggregation Value',
                value=None
            ),
            html.Div(children=[
                dcc.Graph(id='hist_timeline_fig')])
            ])
    ])


def history_compare():
    return htmlDiv(children=[
        html.Div(children=[
            html.H2("Historical Comparison"),
            dcc.Dropdown(
                id='date_to_compare',
                options=[{'label': d, 'value': d} for d in df_hist['date'].unique()],
                multi=False,
                placeholder='Date to Compare To',
                value=df_cov.iloc[0]['date']
            ),
            dcc.Dropdown(
                id='feat_to_compare',
                options=[{''}]
            )
        ])
    ])


# Sequentially add page components to the app's layout
def dynamic_layout():
    return html.Div([
        timeline_vis(),
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



@app.callback(
    [dash.dependencies.Output('hist_agg_val_dd', 'options'),
     dash.dependencies.Output('hist_agg_val_dd', 'value')],
    dash.dependencies.Input('hist_agg_feat_dd', 'value')
)
def update_agg_val_options(agg_feat):
    options = [{'label': val, 'value': val} for val in df_hist[agg_feat].unique()]
    # print(options)
    # print(options[0]['label'])
    value = options[0]['value']
    return options, None


@app.callback(
    dash.dependencies.Output('hist_timeline_fig', 'figure'),
    [dash.dependencies.Input('hist_feature_dd', 'value'),
     dash.dependencies.Input('hist_agg_feat_dd', 'value'),
     dash.dependencies.Input('hist_agg_val_dd', 'value')]
)
def update_timeline_vis(plot_feature, agg_feature, agg_value):
    hist_time_feature = 'date' # can put in db_info
    df_hist_agg = df_hist[df_hist[agg_feature]==agg_value]
    # print(df_hist_agg.head(2))
    
    fig = None
    fig=px.scatter(df_hist_agg, x=hist_time_feature, y=plot_feature)

    fig.update_layout(template='plotly_dark', title=f'Historical Timeline of {plot_feature} Over {agg_feature}',
                          plot_bgcolor='#23272c', paper_bgcolor='#23272c')
    return fig



if __name__ == '__main__':
    app.run_server(debug=True, port=1050, host='0.0.0.0')
