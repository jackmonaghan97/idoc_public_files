
import duckdb
import pandas as pd

def extract_pgres() -> pd.DataFrame:

        # upload to duckdb
        db_path = r"C:\Users\jackm\OneDrive\Documents\duckdb_cli-windows-amd64\my_database.duckdb"
        conn = duckdb.connect(db_path)

        return conn.execute(f"SELECT * FROM {'tableau_idoc_sentencing'}").df()

if __name__ == "__main__":

        df = extract_pgres()
        df.to_csv('data/prison_data.csv', index=False)
