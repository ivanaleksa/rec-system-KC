import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()


def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(os.getenv("DB_CONN"))
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

def load_features() -> pd.DataFrame:
    df = batch_load_sql(f'SELECT * FROM {os.getenv("FEATURES_LOCATION")}')
    return df

def load_posts() -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT * FROM public.post_text_df;",
        os.getenv("DB_CONN")
    )
    return df
