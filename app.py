import os
import csv
import shutil
import argparse
import textwrap
from confluent_kafka import Consumer, KafkaException, TopicPartition
from vertica_python import connect


def init_directory(folder_path):
    """
    Initialize the given directory. If it exists, clean its contents.
    """
    if os.path.exists(folder_path):
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
    else:
        os.makedirs(folder_path)
    print(f"Directory ready: {folder_path}")


def fetch_query_results(vertica_config, query):
    """
    Execute a query on Vertica and return the results.
    """
    try:
        with connect(**vertica_config) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
        return results
    except Exception as e:
        print(f"Error executing query: {e}")
        return []


def generate_backup_shell_script(query_results, conf_file):
    """
    Generate a shell script from Vertica query results for vkconfig microbatch update.
    """
    with open("./results/backup_kafka_micro_batch.sh", "w") as script_file:
        script_file.write("#!/bin/bash\n\n")
        for row in query_results:
            topic_name = row[0]
            topic_partition = row[1]
            topic_offset = row[2]
            script_file.write(
                f"/opt/vertica/packages/kafka/bin/vkconfig microbatch --update "
                f"--conf {conf_file} --microbatch {topic_name} "
                f"--offset {topic_offset} --partition {topic_partition}\n"
            )
    print(f"Shell script created: backup_kafka_micro_batch.sh")


def fetch_and_write_vertica_results(vertica_config, query, output_csv):
    """
    Run a Vertica query, return the results, and write them to a CSV file.
    """
    try:
        with connect(**vertica_config) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            with open(os.path.join("results/", output_csv), mode='w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(results)

            print(f"Results written to {output_csv}")
            return results
    except Exception as e:
        print(f"Error running Vertica query: {e}")
        return []


def get_offsets(broker, topic, partition, timestamp):
    """
    Get the closest Kafka offset for a given timestamp.
    """
    consumer_config = {
        "bootstrap.servers": broker,
        "group.id": "temporary_group",
        "enable.auto.commit": False
    }
    consumer = Consumer(consumer_config)

    try:
        partitions = consumer.list_topics(topic).topics[topic].partitions.keys()
        topic_partitions = [TopicPartition(topic, partition, timestamp) for partition in partitions]
        offsets_for_times = consumer.offsets_for_times(topic_partitions, timeout=10)
        offsets = {}

        for tp in offsets_for_times:
            low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition))
            if tp.offset == -1:
                offsets[tp.partition] = low
            else:
                offsets[tp.partition] = tp.offset

        return offsets
    except KafkaException as e:
        print(f"Kafka Exception: {e}")
        print(f"Error details for topic {topic}, partition {partition}, timestamp {timestamp}")
        return {}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {}
    finally:
        consumer.close()


def generate_shell_script(output_path, topics_offsets, conf_file):
    """
    Generate a shell script with vkconfig microbatch updates for given offsets.
    """
    with open(os.path.join("results/", output_path), "w") as f:
        for topic, partitions_offsets in topics_offsets.items():
            for partition, offset in partitions_offsets.items():
                line = (
                    f"/opt/vertica/packages/kafka/bin/vkconfig microbatch --update "
                    f"--conf {conf_file} --microbatch {topic} "
                    f"--offset {offset} --partition {partition}\n"
                )
                f.write(line)


def get_closest_offsets_for_topics(broker, topics, timestamp):
    """
    For each topic, get closest offset, low and high offsets for a given timestamp.
    """
    consumer_config = {
        "bootstrap.servers": broker,
        "group.id": "temporary_group",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_config)

    all_offsets = {}
    for topic in topics:
        partitions = consumer.list_topics(topic).topics[topic].partitions.keys()
        topic_partitions = [TopicPartition(topic, partition, timestamp) for partition in partitions]
        offsets_for_times = consumer.offsets_for_times(topic_partitions, timeout=10)

        topic_offsets = {}
        for tp in offsets_for_times:
            low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition))
            if tp.offset == -1:
                topic_offsets[tp.partition] = {
                    "closest": low,
                    "low": low,
                    "high": high,
                    "reason": "No messages found, using earliest available offset",
                }
            else:
                topic_offsets[tp.partition] = {
                    "closest": tp.offset,
                    "low": low,
                    "high": high,
                    "reason": "Exact offset found",
                }

        all_offsets[topic] = topic_offsets

    consumer.close()
    return all_offsets


def get_topics_with_prefix(broker, prefix):
    """
    Return Kafka topics that start with a given prefix.
    """
    consumer_config = {
        "bootstrap.servers": broker,
        "group.id": "temporary_group",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_config)
    topics = consumer.list_topics(timeout=10).topics
    consumer.close()

    matched_topics = [topic for topic in topics if topic.startswith(prefix)]
    return matched_topics


def add_kafka_check_arguments(parser):
    """
    Add CLI arguments for Kafka offset checking.
    """
    parser.add_argument("--broker", required=True, help="Kafka broker address (example: localhost:9092)")
    parser.add_argument("--prefix", required=True, help="Topic name prefix")
    parser.add_argument("--timestamp", required=True, help="Epoch timestamp in milliseconds")


def add_common_arguments(parser):
    """
    Add common CLI arguments for Vertica-Kafka operations.
    """
    parser.add_argument("--broker", required=True, help="Kafka broker address (example: localhost:9092)")
    parser.add_argument("--vertica-host", required=True, help="Vertica host address")
    parser.add_argument("--vertica-user", required=True, help="Vertica username")
    parser.add_argument("--vertica-password", required=True, help="Vertica password")
    parser.add_argument("--vertica-db", required=True, help="Vertica database name")
    parser.add_argument("--output", default="vertica_results.csv", help="CSV file name for Vertica results")
    parser.add_argument("--conf", required=True, help="WeBlog.conf file path")
    parser.add_argument("--script-output", default="offset_script.sh", help="Output shell script file name")
    parser.add_argument("--timestamp", required=True, type=int, help="Epoch timestamp in milliseconds for offset lookup")


def handle_switch_a(args):
    """
    Handle switch A: Fetch Vertica microbatch history, query Kafka offsets,
    and generate vkconfig update scripts.
    """
    print(f"Handling Switch A with args: {vars(args)}")
    vertica_config = {
        "host": args.vertica_host,
        "user": args.vertica_user,
        "password": args.vertica_password,
        "database": args.vertica_db,
        "port": 5433
    }
    init_directory('results')

    query = textwrap.dedent("""
        SELECT source_name, source_partition, end_offset, batch_end
        FROM (
            SELECT
                source_name,
                source_partition,
                end_offset,
                end_reason,
                batch_end,
                ROW_NUMBER() OVER (PARTITION BY source_name, source_partition ORDER BY batch_end DESC) AS row_num
            FROM stream_config.stream_microbatch_history
            WHERE source_cluster = '<clustername>' AND end_reason = 'END_OF_STREAM'
        ) subquery
        WHERE row_num = 1
        """).strip()

    results = fetch_and_write_vertica_results(vertica_config, query, args.output)
    if not results:
        print("No data fetched from Vertica query.")
        return

    topics_offsets = {}
    for source_name, source_partition, end_offset, _ in results:
        offsets = get_offsets(args.broker, source_name, source_partition, args.timestamp)
        if offsets:
            topics_offsets[source_name] = offsets

    if topics_offsets:
        generate_shell_script(args.script_output, topics_offsets, args.conf)
        print(f"Shell script created: {args.script_output}")
    else:
        print("No offsets retrieved for any topic.")

    query_results = fetch_query_results(vertica_config, query)
    if not query_results:
        print("No results or error during query execution.")
        return

    generate_backup_shell_script(query_results, args.conf)


def handle_get_offsets(args):
    """
    Handle get_offsets: Find topics by prefix and print closest/low/high offsets for each partition.
    """
    print(f"Handling get offsets args: {vars(args)}")

    try:
        timestamp = int(args.timestamp)
    except ValueError:
        print("Error: Provide a valid epoch timestamp in milliseconds.")
        return

    matched_topics = get_topics_with_prefix(args.broker, args.prefix)
    if not matched_topics:
        print(f"No topics found with prefix '{args.prefix}'")
        return

    print(f"Found topics: {', '.join(matched_topics)}")

    offsets = get_closest_offsets_for_topics(args.broker, matched_topics, timestamp)

    print("\nOffset Information:")
    for topic, partitions in offsets.items():
        print(f"\nTopic: {topic}")
        for partition, offset_data in partitions.items():
            print(
                f"  Partition {partition}: "
                f"Closest={offset_data['closest']}, "
                f"Low={offset_data['low']}, "
                f"High={offset_data['high']} "
                f"({offset_data['reason']})"
            )


def create_parser():
    """
    Create the CLI argument parser with subcommands for Vertica-Kafka operations.
    """
    parser = argparse.ArgumentParser(description="Vertica Kafka Operations CLI")

    subparsers = parser.add_subparsers(title="Vertica Kafka Operations", dest="site_switch", required=True)

    parser_a = subparsers.add_parser(
        "vkconfig__switchA",
        help="Create vkconfig shell script to update offsets for microbatch topics"
    )
    add_common_arguments(parser_a)
    parser_a.set_defaults(func=handle_switch_a)

    parser_c = subparsers.add_parser(
        "get_offsets_to_prefix",
        help="Get closest, high and low offsets for topics matching a prefix"
    )
    add_kafka_check_arguments(parser_c)
    parser_c.set_defaults(func=handle_get_offsets)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
