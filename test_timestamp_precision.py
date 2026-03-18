"""Quick script to prove that DM Instances API and Streams API handle timestamp precision differently.

Usage:
    python test_timestamp_precision.py \\
        --stream-id <stream_external_id> \\
        --instance-container-space <space> --instance-container-id <external_id> \\
        --record-container-space <space> --record-container-id <external_id> \\
        --timestamp-property <property_name> \\
        --instance-space <space>

Both containers must have a timestamp property with the given name.
"""

import argparse

from dotenv import load_dotenv
from cognite.client.utils import ms_to_datetime
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

TIMESTAMP_MS = 1666687298035
dt = ms_to_datetime(TIMESTAMP_MS)
timestamp_6_digits = dt.isoformat()  # 2022-10-25T08:41:38.035000+00:00
timestamp_3_digits = dt.isoformat(timespec="milliseconds")  # 2022-10-25T08:41:38.035+00:00


def test_dm_instances(
    client: ToolkitClient,
    instance_space: str,
    container_space: str,
    container_external_id: str,
    prop_name: str,
) -> None:
    print("=== DM Instances API ===")
    external_id = "_toolkit_timestamp_precision_test"
    body = {
        "items": [
            {
                "instanceType": "node",
                "space": instance_space,
                "externalId": external_id,
                "sources": [
                    {
                        "source": {
                            "type": "container",
                            "space": container_space,
                            "externalId": container_external_id,
                        },
                        "properties": {
                            prop_name: timestamp_6_digits,
                        },
                    }
                ],
            }
        ],
        "autoCreateDirectRelations": False,
        "skipOnVersionConflict": False,
        "replace": True,
    }
    try:
        response = client.post(
            url=f"/api/v1/projects/{client.config.project}/models/instances",
            json=body,
        )
        print(f"  Status: {response.status_code}")
        if response.status_code >= 400:
            print(f"  6-digit timestamp REJECTED: {response.json()}")
        else:
            print(f"  6-digit timestamp ACCEPTED by DM Instances API")
    except Exception as e:
        print(f"  6-digit timestamp REJECTED by DM Instances API: {e}")

    try:
        client.post(
            url=f"/api/v1/projects/{client.config.project}/models/instances/delete",
            json={"items": [{"instanceType": "node", "space": instance_space, "externalId": external_id}]},
        )
        print("  (cleaned up test instance)")
    except Exception:
        pass


def test_streams(
    client: ToolkitClient,
    stream_id: str,
    instance_space: str,
    container_space: str,
    container_external_id: str,
    prop_name: str,
) -> None:
    print("=== Streams API ===")
    external_id = "_toolkit_timestamp_precision_test"

    for label, timestamp_value in [("6-digit", timestamp_6_digits), ("3-digit", timestamp_3_digits)]:
        body = {
            "items": [
                {
                    "externalId": external_id,
                    "space": instance_space,
                    "sources": [
                        {
                            "source": {
                                "type": "container",
                                "space": container_space,
                                "externalId": container_external_id,
                            },
                            "properties": {
                                prop_name: timestamp_value,
                            },
                        }
                    ],
                }
            ]
        }
        try:
            response = client.post(
                url=f"/api/v1/projects/{client.config.project}/streams/{stream_id}/records",
                json=body,
            )
            print(f"  {label} ({timestamp_value}): Status {response.status_code}")
            if response.status_code >= 400:
                print(f"    REJECTED: {response.json()}")
            else:
                print(f"    ACCEPTED")
        except Exception as e:
            print(f"  {label} ({timestamp_value}): REJECTED: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream-id", required=True)
    parser.add_argument("--instance-container-space", required=True)
    parser.add_argument("--instance-container-id", required=True)
    parser.add_argument("--record-container-space", required=True)
    parser.add_argument("--record-container-id", required=True)
    parser.add_argument("--timestamp-property", required=True)
    parser.add_argument("--instance-space", required=True)
    args = parser.parse_args()

    load_dotenv(override=True)
    env = EnvironmentVariables.create_from_environment()
    client = env.get_client()
    print(f"Project: {client.config.project}")
    print(f"6-digit (microseconds): {timestamp_6_digits}")
    print(f"3-digit (milliseconds): {timestamp_3_digits}")
    print()

    test_dm_instances(client, args.instance_space, args.instance_container_space, args.instance_container_id, args.timestamp_property)
    print()
    test_streams(client, args.stream_id, args.instance_space, args.record_container_space, args.record_container_id, args.timestamp_property)
