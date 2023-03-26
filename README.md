# **Prefect**

### **Setup environment**

Let’s first create a conda environment. We need a python requirements file.

**File `requirements.txt`**

```bash
pandas==1.5.2
prefect==2.7.7
prefect-sqlalchemy==0.2.2
prefect-gcp[cloud_storage]==0.2.4
protobuf==4.21.11
pyarrow==10.0.1
pandas-gbq==0.18.1
psycopg2-binary==2.9.5
sqlalchemy==1.4.46
```

Commands for creating environment

```bash
conda create --name zoomcamp
conda activate zoomcamp
pip install -r requirements.txt
```

### **Ingestion without prefect**

File **`ingest-data.py`**

```python
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def ingest_data(user, password, host, port, db, table_name, csv_url):

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {csv_url} -O {csv_name}")
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)
```

> Note: I have used docker containers to do all of this. The file to replicate this docker containers is `docker-compose.yaml`
> 

Then, I executed the python program.

```python
python ingest_data.py
```

- After successful execution, data will be there in the `ny_taxi` table.

### **Ingestion with prefect**

**File `ingest_data_flow.py`**

```python
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if csv_url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    ##os.system(f"wget {csv_url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    return df 

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    
    return df 

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, df):
    
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Flow")
def main(table_name: str):
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':
    main("yellow_taxi_trips")
```

Start the Prefect Orion orchestration engine.

```python
prefect orion start
```

Open another terminal window and run the following command:

```python
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Go to localhost 4200 to open the Orion interface.

You can consult a lot of information about our performances.

This script runs just fine but instead of hard coding our postgres connection parameters we can utilize the functionality provided by prefect named **Blocks.**

Blocks are reusable and also follow a low-code approach.

Open Orion UI select blocks and select the below-given option.

![Untitled](https://github.com/pratik-18/Prefect/blob/main/images/Untitled.png)

After that provide the parameters as follows:

- **Block Name**: postgres-connector
- **Driver**: SyncDriver
- **The driver name to use**: postgresql+psycopg2
- **The name of the database to use**: ny_taxi
- **Username**: root
- **Password**: root
- **Host**: localhost
- **Port**: 5432

Then click on the **Create** button

The Orion interface provides us with instructions to add to our python code.

```python
from prefect_sqlalchemy import SqlAlchemyConnector

with SqlAlchemyConnector.load("postgres-connector") as database_block:
    ...
```

Then executed the python program. 

```python
python ingest_data_flow_block.py
```

For more details regarding execution checkout Orion UI & how for data check out Postgres 

### **ETL with GCP & Prefect**

**File `etl_web_to_local.py`**

This file will read the data from the given CSV file, clean it & generate a parquet file locally out of transformed data. 

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    ## Read taxi data from file and put it into pandas DataFrame
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    ## Fix dtype issues
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    ## Write DataFrame out locally as parquet file
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@flow()
def etl_web_to_local() -> None:
    ## The main ETL function
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    write_local(df_clean, color, dataset_file)

if __name__ == "__main__":
    etl_web_to_local()
```

Now the goal is to everything the same but instead of storing file locally we’ll put data on Google Cloud Storage. For that first we need do follow the following steps:

1. **Create a bucket -** Select **Cloud Storage**, and select **Buckets** & provide necessary details.
2. **Create a service account** **-** Because we need to provide credentials to create a Block in Orion UI that will help us connect to Google Cloud Storage and put data there. 
    - On **Google Cloud Console**, select **IAM & Admin**, and **Service Accounts**. Then click on **+ CREATE SERVICE ACCOUNT and** provide the necessary information
    - In the next step, it will ask for information regarding the role. Give the roles **BigQuery Admin** and **Storage Admin**.
    - Click on the **CONTINUE** button & then **DONE**
3. **Keys for the service account -** Lastly we’ll generate keys that we’ll provide to Orion UI
    - Click on **ADD KEY +**button, select **CREATE A NEW KEY**, select **JSON** and click on **CREATE**  button.

**Now let’s create some Blocks to connect to Google Cloud Storage.**

- **GCP credential Block** that we need to provide in order to create GCS Bucket block.
    
    ![Untitled](https://github.com/pratik-18/Prefect/blob/main/images/Untitled%201.png)
    
    - Provide the credentials that we generated above.
    
- **GCP Bucket Block** this we’ll use in the code
    
    ![Untitled](https://github.com/pratik-18/Prefect/blob/main/images/Untitled%202.png)
    
    After doing so Orion UI will provide us with code that we need to put in our code.
    

Now code will look as follow:

**File `etl_web_to_local.py`**

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
## from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    ## Read taxi data from web into pandas DataFrame
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    ## Fix dtype issues
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    ## Write DataFrame out locally as parquet file
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None :
    ## Upload local parqet file to GCS
    gcs_block = GcsBucket.load("dtc-bucket")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)

@flow()
def etl_web_to_gcs() -> None:
    ## The main ETL function
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()
```

> Note: Names in the screenshot and actual code are bit different as I experimented afterwards.
> 

Now run the python program & checkout Bucket a file similar to local file will be there.

### F**rom Google Cloud Storage to Big Query**

**1)** **One option is to import data from GCS to Big Query using UIs**

For that follow the following steps:

1. Search & Select **Big Query**
2. Select **Add Data**
3. Select file from the **GCS**
4. In the the destination it will ask for the **Data Set** so here we are creating the new **Data Set**
    1. Select create **Data Set**
    2. Provide ID 
    3. Select **Create**
5. Provide **Table** Name

After it finishes processing data can be queried.

****2)We can also do this using Script****

First delete the data that we just loaded using Delete Query.

We are going to use **[pandas.DataFrame.to_gbq](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_gbq.html)** function. Code will look as follows.

**File `etl_gcs_to_bq.py`**

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color : str, year : int, month : int) -> Path:
    ## Download trip data from GCS -> Here we are simulation that we are having data in the data lack that we are tryping 
    ## gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = "data\yellow\yellow_tripdata_2021-01.parquet"
    gcs_block = GcsBucket.load("dtc-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    ## Data Cleaning
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("dtc-de-user")
    df.to_gbq(
        destination_table="dtc_bq.yellow_taxi_trips",
        project_id="dtc-de-k",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    ## Main ETL Flow to load data from GCS to Biq Query
    color = "yellow"
    year = 2021
    month = 1
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()
```

After running script run the following query in query interface on Google Cloud and run this query.

```python
SELECT count(1) FROM `dtc_bq.yellow_taxi_trips`;
```

This should return **1369765**.

### Parameterized Workflow & Deployment

**Parameterizing**

Parameterizing flow is the same as passing parameters to a function code will look as follows:

**File `parameterized_flow.py`**

```python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
## from random import randint

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    ## Read taxi data from web into pandas DataFrame
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    ## Fix dtype issues
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    ## Write DataFrame out locally as parquet file
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None :
    ## Upload local parqet file to GCS
    gcs_block = GcsBucket.load("dtc-bucket")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    ## The main ETL function
    ## color = "yellow"
    ## year = 2021
    ## month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)
```

**Deployment**

1. To deploy a pipeline firstly we have to **create a deployment**

To create the deployment for Pipeline

```powershell
Syntex:
prefect deployment build ./PYTHON_FILE.py:ENTRY_POINT_FUNCTION -n "NAME OF PIPELINE"

Ex:
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```

- After successful execution, we’ll receive one .yaml file.

1. Since our flow takes parameters we need to edit the .yaml file to **provide** **parameters**
    
    Edit the parameter alternatively we can provide it from the Orion UI as well.
    
    ```yaml
    parameters: {"color":"yellow", "months":[1, 2, 3], "year":2021}
    ```
    
    After editing values for key `parameters` it should look as above & file is given below for the reference:
    
    **File `etl_parent_flow-deployment.yaml`**
    

1. Apply the Deployment
    
    The send all the metadata to prefect for orchestration:
    
    ```powershell
    prefect deployment apply YAML_FILE.yaml
    ```
    
    We can navigate to the UI to see the deployment.
    

1. In order to execute the flow we **need an agent** that will execute the pipeline otherwise Flow will go `schedule` stage (Because no one is there to execute it)
    
    
    As of now we are using a local agent as well but it could be docker, VM etc.
    
    we can create a local agent as follow.
    
    ```powershell
    prefect agent start --work-queue "default"
    ```
    

1. Execute Pipeline
    
    After having the agent activated we can execute Pipeline from Prefect UI or even schedule
