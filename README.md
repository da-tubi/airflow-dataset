# Pants Airflow Minimal
This repo could be separated into two parts:
+ The Airflow Infra described below
+ The demo dags to explore airflow dataset
  + [DONE] Partition: `data_interval_start | ds`
  + [DONE] Backfill
    + clear can not trigger dataset
    + manually trigger can trigger dataset
  + [DONE] How to trigger by multiple datasets
  + [DONE] daily to weekly
  + [DONE] when not triggerred, should be an alert

## Install Pants and Python
1. Install Pants: https://www.pantsbuild.org/docs/installation
2. Install Python 3.10.x
   + Because in [pants.toml](pants.toml), we set the intepreter to Python 3.10.x
   + Because on Ubuntu 22.04, the default Python is Python 3.10.x
3. No Python Virtual Environment needed

If `apt install` or `brew install` does not work, You can install any version of Python via:
```
xmake l python.lua
```

## One Step Guide to launch Airflow
Bash:
```
bin/install_airflow && bin/airflow_config && airflow standalone
```

Elvish:
```
bin/launch
```

## Step-by-step Guide to launch Airflow
### Step 1: Build the package and install it to `$HOME/airflow/bin/python`
``` bash
bin/install_airflow
```

### Step 2: Airflow Config
``` bash
bin/airflow_config
```
It will:
+ Set `core.dags_folder` to the PEX user code dynamically
+ Set `core.load_examples` to `False`


### Step 3: Launch Airflow
```
airflow standalone
```
If the command line `airflow` is not available, please add `$HOME/bin` to `$PATH`.

For more info, see https://airflow.apache.org/docs/apache-airflow/stable/start.html


## How to remove all the config and get a clean environment
Juse remove the $AIRFLOW_HOME:
```
rm -rf $HOME/airflow
# bin/airflow_reset
```
