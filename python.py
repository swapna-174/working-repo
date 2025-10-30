import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException
from pprint import pprint
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import String, Integer, Float, DateTime, Boolean
import urllib

# Set region and authenticate
region = PureCloudPlatformClientV2.PureCloudRegionHosts.us_west_2
PureCloudPlatformClientV2.configuration.host = region.get_api_host()

client_id = '57a604b2-1c4b-4fa1-b84a-d30dc733f48a'
client_secret = 'bjXobAVFqcHoD-0fpr-rH9MRjoJOaG_6ic71YQJkQ9k'

api_client = PureCloudPlatformClientV2.api_client.ApiClient()
api_client.get_client_credentials_token(client_id, client_secret)

# Create Conversations API instance
conversations_api = PureCloudPlatformClientV2.ConversationsApi(api_client)

# Specify conversation IDs
#conversation_ids = ['80b9745d-0341-4591-a428-ba9d81068bfc']

# Define time range (last 7 days)
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=30)

start_str = start_time.isoformat()
end_str = end_time.isoformat()

# Pagination setup
page_number = 1
page_size = 100
all_conversations = []

while True:
    query_body = {
        "interval": f"{start_str}/{end_str}",
        "paging": {
            "pageSize": page_size,
            "pageNumber": page_number
        }
    }

    try:
        response = conversations_api.post_analytics_conversations_details_query(query_body)
        conversations = response.conversations
        all_conversations.extend(conversations)

        print(f"Fetched page {page_number} with {len(conversations)} conversations.")

        if len(conversations) < page_size:
            break  # Last page reached

        page_number += 1

    except ApiException as e:
        print(f"Error fetching conversations: {e.status} - {e.reason}")
        break

# Convert conversation objects to dictionaries
conversation_dicts = [convo.to_dict() for convo in all_conversations]

# Create a DataFrame
df = pd.DataFrame(conversation_dicts)

# Generate dtype mapping for SQL Server
dtype_mapping = {}
for column, dtype in df.dtypes.items():
    if pd.api.types.is_integer_dtype(dtype):
        dtype_mapping[column] = Integer()
    elif pd.api.types.is_float_dtype(dtype):
        dtype_mapping[column] = Float()
    elif pd.api.types.is_bool_dtype(dtype):
        dtype_mapping[column] = Boolean()
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        dtype_mapping[column] = DateTime()
    else:
        dtype_mapping[column] = String(255)

# Define SQL Server connection string using trusted connection
driver_name = 'ODBC Driver 17 for SQL Server'
server_name = 'BISSAS02PV'
database_name = 'SNOW_Data'
IF_EXISTS_MODE = 'replace'   # or 'append

conn_str = (
    f"DRIVER={{{driver_name}}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"Trusted_Connection=yes;"
)

conn_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"

# Create SQLAlchemy engine and export to SQL Server with error handling
try:
    engine = create_engine(conn_url, connect_args={"connect_timeout": 90})
    df.to_sql('GenesysConversations_data', con=engine, if_exists=IF_EXISTS_MODE, index=False, dtype=dtype_mapping)
    print("Data has been successfully exported to SQL Server table 'GenesysConversations_data")
except SQLAlchemyError as db_err:
    print(f"Database connection or export failed: {db_err}")
