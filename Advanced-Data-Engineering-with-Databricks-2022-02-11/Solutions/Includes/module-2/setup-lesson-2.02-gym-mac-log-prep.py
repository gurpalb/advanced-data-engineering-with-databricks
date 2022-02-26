# Databricks notebook source
# MAGIC %run ../_databricks-academy-helper $lesson="2.02"

# COMMAND ----------

# MAGIC %run ../_utility-functions

# COMMAND ----------

gym_mac_logs_source = f"{DA.data_source_uri}/gym-logs"
DA.paths.gym_mac_logs_json = f"{DA.paths.working_dir}/gym_mac_logs.json"

def install_gym_logs():
    # Remove existing files
    # dbutils.fs.rm(DA.paths.gym_mac_logs_json, True)

    # Copies files to demo directory
    files = dbutils.fs.ls(gym_mac_logs_source)
    for curr_file in [file.name for file in files if file.name.startswith(f"2019120")]:
        dbutils.fs.cp(f"{gym_mac_logs_source}/{curr_file}", f"{DA.paths.gym_mac_logs_json}/{curr_file}")


# COMMAND ----------

class DataFactory:
    def __init__(self, source, userdir):
        self.source = source
        self.userdir = userdir
        self.curr_day = 10
    
    def load(self, continuous=False):
        files = dbutils.fs.ls(self.source)
        if self.curr_day > 16:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.curr_day <= 16:
                print(f"Loading day #{self.curr_day}")
                for curr_file in [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]:
                    print(f"...{curr_file}")
                    dbutils.fs.cp(f"{self.source}/{curr_file}", f"{self.userdir}/{curr_file}")
                self.curr_day += 1
            print("Data source exhausted")
        else:
            print(f"Loading day #{self.curr_day}")
            for curr_file in [file.name for file in files if file.name.startswith(f"201912{self.curr_day}")]:
                print(f"...{curr_file}")
                dbutils.fs.cp(f"{self.source}/{curr_file}", f"{self.userdir}/{curr_file}")
            self.curr_day += 1

# COMMAND ----------

DA.cleanup()
DA.init()

install_gym_logs()
DA.data_factory = DataFactory(gym_mac_logs_source, DA.paths.gym_mac_logs_json)

DA.conclude_setup()

