from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column
import pandas as pd
from sqlalchemy import create_engine
import urllib
import io

# Login through U/P, not using token
tableau_server_config = {
    'tableau_dev' : {
        'server': '',
        'api_version': '3.14',
        'username':'',
        'password':'',
        'site_name': '',
        'site_url': ''
    }
}

Date_filter = urllib.parse.quote('Ext Date')
Month_filter = urllib.parse.quote('Relevant Month')
Code = urllib.parse.quote('Area Code')

conn = TableauServerConnection(config_json = tableau_server_config, env = 'tableau_dev', ssl_verify = False)
response = conn.sign_in()

response

Ext_Date =  urllib.parse.quote("2022-07-04")
Relevant_Month =  urllib.parse.quote("2022-06-01")
Area_Code =  urllib.parse.quote("IND")

param_dict = { "Ext Date": f"vf_{Date_filter}={Ext_Date}",
                "Relevant Month": f"vf_{Month_filter}={Relevant_Month}"},
                "Area Code": f"vf_{Code}={Area_Code}" }
                

views_df = querying.get_views_dataframe(conn)
view_df = flatten_dict_column(views_df, keys = ["name", "id"], col_name="workbook")
views_df.head(10)

views_df = views_df[views_df["workbook_name"] == "Month Results"]
views_df_new = views_df[views_df["viewUrlName"] == "Results"]

views_df.head(10)

VIEW_ID = "6v2q12-baa4-3436-v2ws-aswdtgfcqagh"

views_name_df = querying.get_view_data_dataframe(conn, view_id=VIEW_ID, parameter_dict=param_dict)

views_name_df.head(10)

db_connect = create_engine('mqsql+mysqlconnector://----')