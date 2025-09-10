# vkconfig-set-offset
This script fetches data from Vertica and writes it to a CSV file, while also retrieving Kafka offset values based on a given timestamp (epoch in milliseconds). It then generates a shell script that can be used for Kafka microbatch operations.

#### Requirements
- Python 3.9 or higher

- Docker (optional)

- Kafka Broker

- Vertica Database

### Getting Started
To install the Python dependencies:
```bash
pip install -r requirements.txt
``` 

### Configuration
You need to set the following environment variables:

- --broker: Kafka broker address (example: localhost:9092)

- --vertica-host: Vertica database server address

- --vertica-user: Vertica username

- --vertica-password: Vertica password

- --vertica-db: Vertica database name

- --output: CSV file name where Vertica query results will be stored (default: vertica_results.csv)

- --conf: Path to Kafka configuration file

- --script-output: Name of the generated shell script file (default: offset_script.sh)

- --timestamp: Timestamp (epoch in milliseconds) used to get the closest Kafka offset

### 3. Running the Script
To run the script, you can use the following command:

```bash
python app.py vkconfig_site_switchA --broker <kafka_broker> --vertica-host <vertica_host> --vertica-user <vertica_user> --vertica-password <vertica_password> --vertica-db <vertica_db> --conf <config_file> --timestamp <timestamp>
```

### 4. Docker Usage (Optional)
If you prefer to run the script using Docker, you can build the Docker image and run it with the necessary environment variables:
```bash
docker build -t vkconfig-set-offset .
docker run -e BROKER=<kafka_broker> -e VERTICA_HOST=<vertica_host> -e VERTICA_USER=<vertica_user> -e VERTICA_PASSWORD=<vertica_password> -e VERTICA_DB=<vertica_db> -e OUTPUT=vertica_results.csv -e CONF=<config_file> -e SCRIPT_OUTPUT=offset_script.sh -e TIMESTAMP=<timestamp> vkconfig-set-offset
``` 
### 5. Results
The script will generate a CSV file with the results from the Vertica query and a shell script   that contains the Kafka offset commands based on the provided timestamp. The shell script can be executed to perform microbatch operations in Kafka.   
 
### 6. Vertica and Kafka Sitesiwtch Configuration
Ensure that your Vertica database and Kafka broker are properly configured and accessible from the machine where you run this script. The Vertica query should be adjusted according to your specific requirements. 

#### 1. VkConfig Shutdown
a.  Kill The VKconfig launching
```shell
ps -aux |grep vkconfig
kill -9 <process-id>
```
b. shutdown VKConfig
```shell
 /opt/vertica/packages/kafka/bin/vkconfig shutdown --conf /home/dbadmin/kafka/kafka.conf
```
#### 2. Change Cluster Hosts
a.  Login Vertica as dbadmin and update cluster as the below query
```shell
 UPDATE config_shcema.stream_clusters
SET hosts = 'kafka01:9092,kafka02:9092,kafka03:9092'
WHERE id = 3000001
```

#### 3. Update micro batches according to new kafka offsets
a- Create micro batches scripts for updating offsets
```shell
<<<<<<< HEAD
python app.py vkconfig__switchA --broker kafka:9092 --vertica-host ****** --vertica-user ****** --vertica-password ******* --vertica-db ****** --output backup_microbatch.csv --conf /home/dbadmin/kafka/kafka.conf --script-output kafka_microbatch.sh --timestamp <Epoch time in milis>
=======

```

b- run the generated query at active vkconfig server

```shell
bash kafka_microbatch.sh
```

Note: If you observe any problem you can change old cluster hosts and run the backup script
```shell
bash backup_kafka_micro_batch.sh
```
#  Operation Checks


```shell
# Cluster Name Check
/opt/vertica/packages/kafka/bin/vkconfig cluster --read --conf /home/dbadmin/kafka/kafka.conf

# SQL checks
select * from
stream_config.stream_clusters;

select source_name, source_partition, start_offset, end_offset, batch_end from
stream_config.stream_microbatch_history
where  source_cluster = '<related cluster name>' and end_reason = 'END_OF_STREAM'
order by batch_end desc limit 100;

select
max(transaction_id) max_transaction_id,
max(batch_start) max_batch_start,
max(frame_start) max_frame_start
from stream_config.stream_microbatch_history smh;

```


#  Kafka Offsets Checking Operations
You can get closest offsets,high and low offsets according to topic prefix name as the below
```shell
python app.py get_offsets_to_prefix --broker kafka:9092 --prefix topic_name --timestamp 1734077786000
```
output:
```shell
Offset Information for prefix 'topic_name' at timestamp 1734077786000:
Kafka Broker: kafka:9092

Topic: topic_name
  Partition 0: Closest=16971509810, Low=16894883406, High=16971560238 (Exact offset found)
  Partition 1: Closest=15580319810, Low=15504596897, High=15580370272 (Exact offset found)
  Partition 2: Closest=15580218415, Low=15503530610, High=15580268893 (Exact offset found)
  Partition 3: Closest=15580742752, Low=15504641164, High=15580793243 (Exact offset found)
  Partition 4: Closest=15580746842, Low=15504610092, High=15580797346 (Exact offset found)
  Partition 5: Closest=15580198173, Low=15503938601, High=15580248677 (Exact offset found)
  Partition 6: Closest=15580724362, Low=15505092015, High=15580774895 (Exact offset found)
  Partition 7: Closest=15580320510, Low=15503721434, High=15580371044 (Exact offset found)
  Partition 8: Closest=15580192389, Low=15503218948, High=15580242949 (Exact offset found)
  
Topic: topic_name
  Partition 0: Closest=123, Low=120, High=125 (Exact offset found)
 
```
