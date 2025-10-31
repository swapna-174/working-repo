import os
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException
from datetime import datetime, timedelta, timezone
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mssql import NVARCHAR, DATETIME2, BIT, BIGINT, FLOAT
import urllib

# --- Config (use env vars!) ---
REGION = PureCloudPlatformClientV2.PureCloudRegionHosts.us_west_2
PureCloudPlatformClientV2.configuration.host = REGION.get_api_host()

CLIENT_ID = os.getenv("GENESYS_CLIENT_ID")
CLIENT_SECRET = os.getenv("GENESYS_CLIENT_SECRET")

SERVER_NAME = "BISSAS02PV"
DATABASE_NAME = "SNOW_Data"
TABLE_NAME = "GenesysConversations_data"
SCHEMA = "dbo"  # change if needed
IF_EXISTS_MODE = "replace"  # or "append"

# --- Auth ---
api_client = PureCloudPlatformClientV2.api_client.ApiClient()
api_client.get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
conversations_api = PureCloudPlatformClientV2.ConversationsApi(api_client)

# --- Time window (last 30 days) ---
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=30)
interval = f"{start_time.isoformat()}/{end_time.isoformat()}"

# --- Pull data (paged) ---
page_number = 1
page_size = 100
all_conversations = []

while True:
    query_body = {
        "interval": interval,
        "paging": {"pageSize": page_size, "pageNumber": page_number}
        # add "order": "asc", "sortBy": "conversationStart" if you want deterministic paging
    }
    try:
        resp = conversations_api.post_analytics_conversations_details_query(query_body)
        convos = resp.conversations or []
        all_conversations.extend([c.to_dict() for c in convos])

        print(f"Fetched page {page_number} with {len(convos)} conversations.")

        if len(convos) < page_size:
            break
        page_number += 1

    except ApiException as e:
        # Print full context if Genesys returns a JSON body
        extra = getattr(e, "body", None)
        print(f"Error fetching conversations: {e.status} - {e.reason}. Details: {extra}")
        raise

if not all_conversations:
    print("No conversations found in the specified interval.")
    # You may want to exit early here.
    
# --- Flatten nested JSON ---
df_raw = pd.DataFrame(all_conversations)
df = json_normalize(all_conversations, sep="__")

# --- Coerce timestamps to datetime where possible ---
for col in df.columns:
    # Heuristic: parse ISO timestamps
    if df[col].dtype == "object":
        sample = df[col].dropna().astype(str).head(5)
        if sample.str.match(r"\d{4}-\d{2}-\d{2}T").all():
            df[col] = pd.to_datetime(df[col], errors="ignore", utc=True)

# --- Build MSSQL dtype map (use NVARCHAR(MAX) for text-ish/object cols) ---
dtype_map = {}
for col, dtype in df.dtypes.items():
    if pd.api.types.is_bool_dtype(dtype):
        dtype_map[col] = BIT
    elif pd.api.types.is_integer_dtype(dtype):
        # conversation IDs, participant counts can exceed INT; use BIGINT
        dtype_map[col] = BIGINT
    elif pd.api.types.is_float_dtype(dtype):
        dtype_map[col] = FLOAT
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        dtype_map[col] = DATETIME2
    else:
        # Default to NVARCHAR(MAX) to avoid truncation of long JSON/text
        dtype_map[col] = NVARCHAR(None)  # renders as NVARCHAR(MAX)

# --- Connection (Windows auth) ---
driver_name = "ODBC Driver 17 for SQL Server"  # or 18 if that's what you have installed
conn_str = (
    f"DRIVER={{{driver_name}}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)
conn_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"

# --- Write to SQL Server ---
try:
    engine = create_engine(
        conn_url,
        connect_args={"connect_timeout": 90},
        fast_executemany=True  # big perf boost with pyodbc
    )
    # chunksize/method='multi' reduces round-trips and memory
    df.to_sql(
        TABLE_NAME,
        con=engine,
        schema=SCHEMA,
        if_exists=IF_EXISTS_MODE,
        index=False,
        dtype=dtype_map,
        chunksize=2000,
        method="multi",
    )
    print(f"Data has been successfully exported to SQL Server table [{SCHEMA}].[{TABLE_NAME}]")
except SQLAlchemyError as db_err:
    # Show the underlying DBAPI error if present
    msg = str(db_err)
    if hasattr(db_err, "orig"):
        msg += f" | DBAPI: {db_err.orig}"
    print(f"Database connection or export failed: {msg}")
    raise

