import pandas as pd
import urllib
import time
from sqlalchemy import create_engine, text

class DataPipeline:
    @staticmethod
    def load_csv_to_sql(csvfile
                        , mapping
                        , driver
                        , server
                        , database
                        , username
                        , password
                        , table_name
                        , schema
                        , mode
                        , chunk_size=10000):
        
        connection_config = (f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"        )
        connection_config_parse = urllib.parse.quote_plus(connection_config)

        connection_string = f"mssql+pyodbc:///?odbc_connect={connection_config_parse}"

        loader = CSVToSQLHandler(connection_string, chunk_size=chunk_size)
        return loader.process(csvfile, mapping, table_name, schema, mode)

class CSVToSQLHandler:
    def __init__(self, connection_string, chunk_size=1000):
        self.engine = create_engine(connection_string, fast_executemany=True)
        self.chunk_size = chunk_size

    def _get_mapping(self, mapping_path):
        """Reads mapping CSV and returns a dictionary {source: target}"""
        df_map = pd.read_csv(mapping_path)
        return dict(zip(df_map['source_col'], df_map['target_col']))

    def process(self, csvfile, mapping, table_name, schema, mode):
        print(f"--- Starting Load [{mode.upper()}]: {csvfile} ---")
        start_time = time.time()
        
        # Load mapping
        column_map = self._get_mapping(mapping)
        total_rows = 0

        try:
            with self.engine.begin() as conn:
                # If mode is replace, we TRUNCATE instead of DROP to keep your schema/indexes intact
                if mode == 'replace':
                    print(f"Truncating table {schema}.{table_name}...")
                    conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
                
                reader = pd.read_csv(csvfile, chunksize=self.chunk_size)
                
                for i, chunk in enumerate(reader):
                    # 1. Rename columns based on mapping
                    # 2. Only keep columns defined in the mapping
                    chunk = chunk.rename(columns=column_map)[list(column_map.values())]
                    
                    # We always use 'append' here because 'replace' would drop the table every loop
                    # We handled the "replace" logic above via TRUNCATE
                    chunk.to_sql(
                        name=table_name,
                        con=conn,
                        schema=schema,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    
                    total_rows += len(chunk)
                    print(f"Batch {i+1} uploaded. Rows so far: {total_rows}")

            duration = round(time.time() - start_time, 2)
            print(f"--- Success! Loaded {total_rows} rows in {duration}s ---")
            return True

        except Exception as e:
            print(f"\n!!! ERROR !!!\n{str(e)}")
            return False

# --- Execution ---
if __name__ == "__main__":
# PARAMETERS
    DB_SERVER = "NJ1CBIDBP01.STGUSA.local"
    DB_NAME = "STG_EDW"
    DB_USER = "adfuser"
    DB_PASSWORD = "#xexhHMispaC9pojTAAB" 
    DB_DRIVER = "ODBC Driver 17 for SQL Server"

    CSV_FILE = r"C:\Users\nagarjuna.bandi\OneDrive - STG Logistics, Inc\Python Projects\thetinyint\samplefiles\search_terms_houston.csv"
    MAP_FILE = r"C:\Users\nagarjuna.bandi\OneDrive - STG Logistics, Inc\Python Projects\thetinyint\samplefiles\search_terms_mapping.csv"
    TARGET_TABLE = "DIM_DOCUWARE_SEARCH_TERMS"
    TARGET_SCHEMA = "dbo"
    LOAD_MODE = "replace" # Options: 'append' or 'replace'

    DataPipeline.load_csv_to_sql(
        csvfile=CSV_FILE,
        mapping=MAP_FILE,
        driver=DB_DRIVER,
        server=DB_SERVER,
        database=DB_NAME,
        username=DB_USER,
        password=DB_PASSWORD,
        table_name=TARGET_TABLE,
        schema=TARGET_SCHEMA,
        mode=LOAD_MODE
    )