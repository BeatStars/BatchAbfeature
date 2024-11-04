import logging
import boto3
import logging
import multiprocessing
from argparse import ArgumentParser
from datetime import UTC, datetime


GLOBAL_LOCK = multiprocessing.Lock()

def get_member_partions():
    file = open("source.txt", mode="r", encoding="utf-8")
    lines = file.readlines()
    file.close()

    for i in range(0, len(lines), 25):
        partition = lines[i : i+25]
        for i in range(len(partition)):
            partition[i] = partition[i].strip()
        yield partition

def append_to_error_file(partition):
    GLOBAL_LOCK.acquire()
    try:
        with open("error.txt", "a") as file:
            file.writelines(["{}\n".format(member_id) for member_id in partition])
    except Exception as e:
        logging.error("Failed to write error cases. %s", partition)
    finally:
        GLOBAL_LOCK.release()

def apply_partition(table_name, abfeature, partition):
    client = boto3.client('dynamodb')

    date = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    requests = []
    for member_id in partition:
        requests.append({
            'PutRequest': {
                'Item': {
                    'id': {
                        'S': 'AB{}{}'.format(member_id, abfeature),
                    },
                    'date': {
                        'S': date,
                    },
                    'feature': {
                        'S': abfeature,
                    },
                    "pk": {
                        "S": member_id
                    }
                },
            },
        })

    try:
        response = client.batch_write_item(
            RequestItems={
                table_name: requests,
            },
        )
        logging.info(response)
    except Exception as e:
        append_to_error_file(partition)
    finally:
        client.close()

def main():
    parser = ArgumentParser(prog='Batch Insert Abfeature')
    parser.add_argument("env", nargs="?", default="dev")
    parser.add_argument("abfeature", nargs="?", default="MIGRATION_STUDIO_2")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )

    table_name = "{}-abfeature".format(args.env)
    logging.info("Table name -> {}".format(table_name))
    
    abfeature = args.abfeature
    logging.info("Abfeature -> {}".format(abfeature))

    with multiprocessing.Pool() as pool:
        processes = []
        for partition in get_member_partions():
            process = pool.apply_async(apply_partition, (table_name, abfeature, partition))
            processes.append(process)

        for process in processes:
            process.get()        

if __name__ == "__main__":
    main()