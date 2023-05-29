import requests
import pandas as pd
import time
from config_bd import bdd_connection, engine_conn


def create_pivot_maillage():
    with bdd_connection() as conn:
        cur = conn.cursor()
        #recupere tout les commune de lapi gouv
        response = requests.get("https://geo.api.gouv.fr/communes")


        print(response.json())

        for row in response.json():

            code_commune = row['code']
            code_departement = row['codeDepartement']
            code_region = row['codeRegion']

            id_commune = row['codeRegion'] + row['codeDepartement'] + row['code']
            id_departement = row['codeRegion'] + row['codeDepartement'] + "00000"
            id_region = row['codeRegion'] + "00" + "00000"

            requeste_insert_commune = f'''INSERT INTO pivot_maillage VALUES ('{id_commune}', '{code_commune}', 'commune')'''
            cur.execute(requeste_insert_commune)
            requeste_insert_departement = f'''INSERT INTO pivot_maillage VALUES ('{id_departement}', '{code_departement}', 'departement')'''
            cur.execute(requeste_insert_departement)
            requeste_insert_region = f'''INSERT INTO pivot_maillage VALUES ('{id_region}', '{code_region}', 'region')'''
            cur.execute(requeste_insert_region)
            conn.commit()




def select_id_mailage(code, niveau):
    try:
        with bdd_connection() as conn:
            with conn.cursor() as cur:
                select = f'''SELECT id FROM pivot_maillage WHERE code = '{code}' AND niveau = '{niveau}';'''
                cur.execute(select)
                res = cur.fetchone()
                if res is None:
                    return None
                return res[0]
    except Exception as e:
        # Handle the exception appropriately (e.g., log the error, raise it, or return a default value)
        print(f"An error occurred: {e}")
        return None

def select_ids_mailage(codes, niveau):
    try:
        with bdd_connection() as conn:
            with conn.cursor() as cur:
                select = '''SELECT code, id FROM pivot_maillage WHERE code = ANY(%s) AND niveau = %s;'''
                cur.execute(select, (codes, niveau))
                results = cur.fetchall()
                id_mailage_mapping = {code: result for code, result in results}
                return id_mailage_mapping
    except Exception as e:
        # Handle the exception appropriately (e.g., log the error, raise it, or return a default value)
        print(f"An error occurred: {e}")
        return None




def create_table_psycopg2(name_table, columns):
    with bdd_connection() as conn:
        cur = conn.cursor()

        create_table = f"""CREATE TABLE IF NOT EXISTS public.{name_table} ()"""
        cur.execute(create_table)
        conn.commit()
        # alter table
        alter_table = f"""ALTER TABLE public.{name_table} ADD COLUMN IF NOT EXISTS id serial PRIMARY KEY"""
        cur.execute(alter_table)
        conn.commit()
        for column in columns:
            alter_table = f"""ALTER TABLE public.{name_table} ADD COLUMN IF NOT EXISTS {column} varchar(255)"""
            cur.execute(alter_table)
            conn.commit()



def add_row_sql(name, data):
    with bdd_connection() as conn:
        cur = conn.cursor()

        setences = "( "
        for value in data:
            setences += f"{value}, "
        setences = setences[:-2]
        setences += " )"

        insert = f"""INSERT INTO public.{name} VALUES {setences}"""
        cur.execute(insert)
        conn.commit()

def make_row_to_insert(row):


    return (row)

def insert_engine():

    df = pd.read_csv("./base_cc_comparateur.csv", sep=";")
    df_meta = pd.read_csv("./meta_base_cc_comparateur.csv", sep=";")
    print(len(df.index))

    #add col a df et lamda
    #df["id_maillage"] = df.loc[:, "CODGEO"].apply(lambda x: select_id_mailage(x, "commune"))

    codes = df["CODGEO"].tolist()
    codes = [str(code) for code in codes]
    id_maillage_mapping = select_ids_mailage(codes, "commune")
    print(id_maillage_mapping)
    df["id_maillage"] = df["CODGEO"].map(id_maillage_mapping)

    time_start = time.time()

    # create table and columns
    name_table = "stats_maille"
    columns = df.columns.tolist()
    columns = [column.lower() for column in columns]

    print(columns)

    create_table_psycopg2(name_table, columns)

    with engine_conn() as conn:

        for i in range(0, len(df.index), 50000):
            time_start_loop = time.time()

            df_insert = df.iloc[i:i + 50000].apply(lambda row: pd.Series(make_row_to_insert(row)), axis=1)

            df_insert.columns = columns

            df_insert.to_sql(name_table, conn, if_exists='append', index=False)
            conn.commit()
            print("time : ", time.time() - time_start_loop)

        print("time end : ", time.time() - time_start)


if __name__ == "__main__":
    #print(select_ids_mailage(["01001", "01002", "2A001"], "commune"))
    insert_engine()