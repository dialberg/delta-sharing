#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -----------------------------------------------------------------------------------------------------------------------
import os
import importlib
import sys

MODULE_PATH = os.getcwd() + r"\delta-sharing\python\delta_sharing\__init__.py"
MODULE_NAME = "_delta_sharing"

spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module 
spec.loader.exec_module(module)

import _delta_sharing
# -----------------------------------------------------------------------------------------------------------------------

# Point to the profile file. It can be a file on the local file system or a file on a remote storage.
profile_file = os.path.dirname(__file__) + "/../open-datasets.share"

# Create a SharingClient.
client = _delta_sharing.SharingClient(profile_file)

# List all shared tables.
print("########### All Available Tables #############")
print(client.list_all_tables())

# Create a url to access a shared table.
# A table path is the profile file path following with `#` and the fully qualified name of a table (`<share-name>.<schema-name>.<table-name>`).
table_url = profile_file + "#delta_sharing.default.owid-covid-data"

# Create Spark with delta sharing connector
from pyspark.sql import SparkSession

my_c_cnc_p = os.getcwd() + r'\delta-sharing\client\target\scala-2.12'
my_c_cnc_n = 'delta-sharing-client_2.12-1.0.0-SNAPSHOT.jar'
my_c_cnc_p = os.path.join(my_c_cnc_p, my_c_cnc_n)
print('MY JAR EXISTS? : {}'.format(os.path.isfile(my_c_cnc_p)))

my_cnc_p = os.getcwd() + r'\delta-sharing\spark\target\scala-2.12'
my_cnc_n = 'delta-sharing-spark_2.12-1.0.0-SNAPSHOT.jar'
my_cnc_p = os.path.join(my_cnc_p, my_cnc_n)
print('MY JAR EXISTS? : {}'.format(os.path.isfile(my_cnc_p)))

# .config('spark.jars.packages', 'io.delta:delta-sharing-spark_2.12:0.6.4')\
# .config('spark.jars', cnc_p)\
# .config('spark.jars','io.delta_delta-sharing-spark_2.12-0.6.4.jar')\

def_cnc_p = os.getcwd() + r'\delta-sharing\examples'
def_cnc_p = os.path.join(def_cnc_p, 'io.delta_delta-sharing-spark_2.12-0.6.4.jar')
print('DEF JAR EXISTS? : {}'.format(os.path.isfile(def_cnc_p)))

c = my_cnc_p + ',' + my_c_cnc_p

spark = SparkSession.builder \
	.appName("delta-sharing-demo") \
	.master("local[*]") \
    .config('spark.jars', c)\
	.getOrCreate()

# Read data using format "deltaSharing"
print("########### Loading delta_sharing.default.owid-covid-data with Spark #############")
df1 = spark.read.format("deltaSharing").load(table_url) \
	.where("iso_code == 'USA'") \
	.select("iso_code", "total_cases", "human_development_index") \
	.show()

# Or if the code is running with PySpark, you can use `load_as_spark` to load the table as a Spark DataFrame.
print("########### Loading delta_sharing.default.owid-covid-data with Spark #############")
data = _delta_sharing.load_as_spark(table_url)
data.where("iso_code == 'USA'") \
	.select("iso_code", "total_cases", "human_development_index").show()