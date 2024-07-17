from substrateinterface import SubstrateInterface
from concurrent.futures import ThreadPoolExecutor, as_completed
import flask
import psycopg2, psycopg2.pool
from psycopg2.extras import RealDictCursor
import requests
import os, glob, sys, traceback
import bs4
import schedule
from jinja2 import Template
import time
import threading
import re
import json
from google.cloud import storage
import pandas as pd 
import gzip
import shutil
from tqdm import tqdm
import subprocess

from bot import main as bot_main

app = flask.Flask(__name__)
pool = psycopg2.pool.SimpleConnectionPool(1, 20, f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']}")

class ConnectionFromPool:
    def __enter__(self):
        self.conn = pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        pool.putconn(self.conn)

data_types_map = {
        "INTEGER": "bigint",
        "FLOAT": "real",
        "STRING": "text",
        "TIMESTAMP": "timestamp",
        "BOOLEAN": "boolean"
}

POLKADOT_STAKE_CONSTANTS = {
    'auctionAdjust': 0.0,
    'auctionMax': 0.0,
    'falloff': 0.05,
    'maxInflation': 0.1,
    'minInflation': 0.025,
    'stakeTarget': 0.75
}

BUCKET_NAME = 'export-bucket'
DOWNLOAD_PATH = '/tmp/gcs_downloads'
PROCESSED_DATA_PATH = '/tmp/processed_data'


# Function to calculate inflation
def calc_inflation(total_staked: float, total_issuance: float, num_auctions: int):
    params = POLKADOT_STAKE_CONSTANTS
    
    auction_adjust = params['auctionAdjust']
    auction_max = params['auctionMax']
    falloff = params['falloff']
    max_inflation = params['maxInflation']
    min_inflation = params['minInflation']
    stake_target = params['stakeTarget']
    
    if total_staked == 0 or total_issuance == 0:
        staked_fraction = 0
    else:
        staked_fraction = total_staked / total_issuance
    
    ideal_stake = stake_target - min(auction_max, num_auctions) * auction_adjust
    ideal_interest = max_inflation / ideal_stake
    
    if staked_fraction <= ideal_stake:
        inflation = 100 * (min_inflation + (staked_fraction * (ideal_interest - (min_inflation / ideal_stake))))
    else:
        inflation = 100 * (min_inflation + ((ideal_interest * ideal_stake - min_inflation) * 2 ** ((ideal_stake - staked_fraction) / falloff)))
    
    return {
        'idealInterest': ideal_interest,
        'idealStake': ideal_stake,
        'inflation': inflation,
        'stakedFraction': staked_fraction,
        'stakedReturn': (inflation / staked_fraction) if staked_fraction != 0 else 0
    }



def notify(message):
    response = requests.get(os.environ['TELEGRAM_URI'] + "&text=" + message)
    response.raise_for_status()
    print(f"{message}", flush=True)


def get_table_names():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix='export/')
    
    table_names = set()
    for blob in blobs:
        # Extract table name from the file path
        filename = os.path.basename(blob.name)
        if filename.endswith('.csv.gz'):
            table_name = filename.split('.')[0]
            table_names.add(table_name)
    return list(table_names)


def download_and_import_blob(blob, table_name, is_create_table=False):
    local_path = os.path.join(DOWNLOAD_PATH, os.path.basename(blob.name))
    line_count = 0

    with open(local_path, 'wb') as file_obj:
        blob.download_to_file(file_obj)
    
    processed_path = os.path.join(PROCESSED_DATA_PATH, os.path.basename(blob.name)).replace('.csv.gz', '.csv')
    with gzip.open(local_path, 'rt') as gz_file:
        with open(processed_path, 'w') as file_obj:
            file_obj.write(gz_file.read())
            line_count = int(os.popen(f"cat {processed_path} | wc -l").read().strip())
        if is_create_table:
            df = pd.read_csv(processed_path, nrows=1)
            create_table(df, table_name)

    db = pool.getconn()
    cur = db.cursor()
    with open(processed_path, 'r') as file:
        cur.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", file)
    db.commit()
    cur.close()
    pool.putconn(db)    
    
    os.remove(local_path)
    os.remove(processed_path)
    return line_count

def get_table(table_name):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=f'export/{table_name}'))
    line_count = 0

    try:
        first_blob = blobs[0]
        download_and_import_blob(first_blob, table_name, is_create_table=True)
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(download_and_import_blob, blob, table_name) for blob in blobs[1:]]
            for future in as_completed(futures):
                line_count += future.result()
                print(f"Imported {line_count} rows into {table_name}", flush=True)
            #free up memory
            del futures
            del blobs
        notify(f"Finished updating Lambda table {table_name}")
    except (StopIteration, IndexError):
        print(f"No blobs found for table {table_name}")
    except Exception as e:
        notify(f"Error occurred while getting table {table_name}: {e}")
        traceback.print_exc()

def create_table(df, table_name):
    db = pool.getconn()
    cur = db.cursor()
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column in df.columns:
        data_type = data_types_map.get(str(df[column].dtype).upper(), 'text')
        # Enclose the column name in double quotes to maintain capitalisation
        create_table_query += f"\"{column}\" {data_type}, "
    create_table_query = create_table_query[:-2] + ")"
    cur.execute(create_table_query)
    db.commit()
    cur.close()
    pool.putconn(db)


def runDailyQueries():
    #glob .sql_daily files
    db = pool.getconn()
    cur = db.cursor()
    for path in sorted(glob.glob("daily_queries/*.sql")):
        with open(path, 'r') as file:
            timestamp = int(time.time())
            query = file.read()
            cur.execute(query)
            db.commit()
            print(f"Executed {path} in {int(time.time()) - timestamp} seconds", flush=True)
    cur.close()
    notify("Finished running daily queries")

@app.route("/table")
def table():
    # get request parameters
    params = flask.request.args
    with ConnectionFromPool() as db:
        with db.cursor(cursor_factory=RealDictCursor) as cur:

            # base query string
            query = 'SELECT * FROM "dashboard" WHERE 1=1'

            # validation checks and query modifications based on input parameters
            if "search" in params and not re.match("^[a-zA-Z0-9_]*$", params['search']):
                return "Search parameter must be alphanumeric"
            elif "search" in params:
                query += f' AND "dashboard"."address" LIKE \'%{params["search"]}%\''

            #match numbers, dots and commas
            if "minStake" in params and not re.match("^[0-9.,]*$", params['minStake']):
                return "minStake parameter must be numeric"
            elif "minStake" in params:
                query += f' AND CAST("dashboard"."currentStake" AS NUMERIC) >= {params["minStake"]}'

            if "maxStake" in params and not re.match("^[0-9.,]*$", params['maxStake']):
                return "maxStake parameters must be numeric"
            elif "maxStake" in params:
                query += f' AND CAST("dashboard"."currentStake" AS NUMERIC) <= {params["maxStake"]}'

            if "activeOnly" in params and int(params["activeOnly"]) == 1: 
                query += f' AND CAST("dashboard"."activeNominator" AS NUMERIC) = 1'

            if "sortColumn" in params and not re.match("^[a-zA-Z0-9_]*$", params['sortColumn']):
                return "Sort column must be alphanumeric"
            elif "sortColumn" in params:
                query += f' ORDER BY CAST("dashboard"."{params["sortColumn"]}" AS NUMERIC) {"ASC" if params["sortUp"] == "1" else "DESC"}'

            if "size" in params and not re.match("^[0-9]*$", params['size']):
                return "Size parameter must be numeric"
            elif "size" in params and "offset" in params and not re.match("^[0-9]*$", params['offset']):
                return "Offset parameter must be numeric"
            elif "size" in params and "offset" in params:
                query += f' LIMIT {params["size"]} OFFSET {params["offset"]}'

            # perform query
            cur.execute(query)
            rows = cur.fetchall()

            cur.execute("CREATE TABLE IF NOT EXISTS search_log (id SERIAL PRIMARY KEY, search_params JSONB, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            cur.execute("INSERT INTO search_log (search_params) VALUES (%s)", (json.dumps(params),))
            db.commit()

    return flask.jsonify(rows)


@app.route("/submit_email", methods=["POST"])
def submit_email():
    email = flask.request.json['email']

    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        return 400, "Invalid email"

    with ConnectionFromPool() as db:
        with db.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS emails (id SERIAL PRIMARY KEY, email TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            cur.execute("INSERT INTO emails (email) VALUES (%s)", (email,))
            db.commit()
    return "OK"

@app.route("/grey")
def grey():
    #return latest subscan data
    with ConnectionFromPool() as db:
        with db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM subscan ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
    return flask.jsonify(row)

@app.route("/blue")
def blue():
    nominators = """
    WITH
        categorized_nominators AS (
            SELECT
                "address",
                "APY",
                "currentStake",
                "lastEraReward",
                CASE
                    WHEN CAST("currentStake" AS NUMERIC) BETWEEN 10000 AND 99999 THEN 'Dolphin'
                    WHEN CAST("currentStake" AS NUMERIC) BETWEEN 1000 AND 9999 THEN 'Fish'
                    WHEN CAST("currentStake" AS NUMERIC) < 1000 THEN 'Shrimp'
                END AS "category"
            FROM "dashboard"
            WHERE "activeNominator" = '1'
        ),
        ranked_nominators AS (
            SELECT
                "address",
                CAST("APY" AS NUMERIC) AS "APY",
                CAST("currentStake" AS NUMERIC),
                "lastEraReward",
                "category",
                ROW_NUMBER() OVER (PARTITION BY "category" ORDER BY CAST("APY" AS NUMERIC) DESC, CAST("currentStake" AS NUMERIC) DESC) AS "row_num"
            FROM categorized_nominators
            WHERE "APY" IS NOT NULL
        )
        SELECT
            "address",
            CAST("APY" AS NUMERIC) AS "APY",
            "currentStake",
            "lastEraReward",
            "category"
        FROM ranked_nominators
        WHERE "row_num" = 1 AND category IS NOT NULL;
    """
    pools = """
    SELECT
      d."address",
      d."APY",
      d."currentStake",
      d."lastEraReward"
    FROM
      "dashboard" as d
    INNER JOIN
      "pools" as p ON p."address" = d."address"
    WHERE
      d."APY" IS NOT NULL
    ORDER BY
      CAST(d."APY" AS NUMERIC) DESC
    LIMIT 1;
    """
    result = {}
    with ConnectionFromPool() as db:
        with db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(nominators)
            rows = cur.fetchall()
            result['nominators'] = rows

            cur.execute(pools)
            rows = cur.fetchall()
            result['pools'] = rows

    return flask.jsonify(result)

def run_export_script():
    try:
        response = subprocess.run(
            ["bash", "export.sh"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(response.stdout.decode('utf-8'))
    except subprocess.CalledProcessError as e:
        notify(f"Error occurred while running export.sh: {e.stderr.decode('utf-8')}")


def updateLambdaTask():
    #recreate download and processed_data folders
    if os.path.exists(DOWNLOAD_PATH):
        shutil.rmtree(DOWNLOAD_PATH)
    os.makedirs(DOWNLOAD_PATH)
    if os.path.exists(PROCESSED_DATA_PATH):
        shutil.rmtree(PROCESSED_DATA_PATH)
    os.makedirs(PROCESSED_DATA_PATH)

    run_export_script()

    table_names = get_table_names()
    notify(f"Found {len(table_names)} tables in Google Cloud Storage")
    
    for table_name in table_names:
        with ConnectionFromPool() as db:
            with db.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            db.commit()

        get_table(table_name)

    notify("Finished updating Lambda tables")
    #runDailyQueries()
    

def updateSubscanTask():
    try:
        #while substrate interface isn't ok, keep trying different RPC nodes
        nodes = [
            "wss://rpc.polkadot.io",
            "wss://rpc.ibp.network/polkadot",
            "wss://polkadot.api.onfinality.io/public-ws",
        ]

        substrate = None

        for node in nodes:
            try:
                substrate = SubstrateInterface(url=node)
                break
            except Exception as e:
                print(f"Failed to connect to {node}: {e}", flush=True)
                continue

        if substrate is None:
            notify("Failed to connect to any RPC node")
            return

        result = {}
        result["era"] = substrate.query("Staking", "CurrentEra", []).value
        result["totalValidatorCount"] = substrate.query("Staking", "CounterForValidators", []).value
        result["currentValidatorCount"] = len(substrate.query("Session", "Validators", []))
        result["totalIssuance"] = int(substrate.query("Balances", "TotalIssuance", []).value)
        result["totalStaked"] = int(substrate.query("Staking", "ErasTotalStake", [result["era"]]).value)
        result["numAuctions"] = int(substrate.query("Auctions", "AuctionCounter", []).value)
        result["inflation"] = str(calc_inflation(result["totalStaked"], result["totalIssuance"], result["numAuctions"])['inflation'])
        result["minimumActiveStake"] = int(substrate.query("Staking", "MinimumActiveStake", []).value)
        result["percentageStaked"] = str(result["totalStaked"] / result["totalIssuance"])

        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=polkadot&vs_currencies=usd')
        result["dotPrice"] = response.json()['polkadot']['usd']
        
        db = pool.getconn()
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS subscan (id SERIAL PRIMARY KEY, era_id INTEGER, timestamp INTEGER, data JSONB)")
        cur.execute("INSERT INTO subscan (era_id, timestamp, data) VALUES (%s, %s, %s)", (result["era"], int(time.time()), json.dumps(result)))
        db.commit()
        cur.close()
        pool.putconn(db)
    except Exception as e:
        notify(f"Error occurred while updating Subscan table: {e}")
        traceback.print_exc() 


def scheduleThread():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule.every().day.at("00:00").do(updateLambdaTask) 
    schedule.every().hour.do(updateSubscanTask)
    threading.Thread(target=scheduleThread).start()
    threading.Thread(target=bot_main).start()
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
