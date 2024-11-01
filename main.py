from datetime import UTC, datetime
from argparse import ArgumentParser

import boto3

def get_member_partions():
    file = open("source.txt", mode="r", encoding="utf-8")
    lines = file.readlines()
    file.close()

    for i in range(0, len(lines), 25):
        partition = lines[i : i+25]
        for i in range(len(partition)):
            partition[i] = partition[i].strip()
        yield partition

def main():
    parser = ArgumentParser(prog='Batch Insert Abfeature')
    parser.add_argument("env", nargs="?", default="dev")
    parser.add_argument("abfeature", nargs="?", default="MIGRATION_STUDIO_2")
    args = parser.parse_args()

    client = boto3.client('dynamodb')

    table_name = "{}-abfeature".format(args.env)
    print("Table name -> {}".format(table_name))

    date = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    
    abfeature = args.abfeature
    print("Abfeature -> {}".format(abfeature))

    for partition in get_member_partions():
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

        response = client.batch_write_item(
            RequestItems={
                table_name: requests,
            },
        )
        print(response)

if __name__ == "__main__":
    main()