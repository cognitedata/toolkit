from cognite_toolkit._cdf_tk.utils import CDFToolConfig


def main() -> None:
    config = CDFToolConfig()
    client = config.client
    all_containers = client.data_modeling.containers.list(limit=-1)
    deleted = client.data_modeling.containers.delete(all_containers.as_ids())
    print(f"Deleted {deleted} containers")


if __name__ == "__main__":
    main()
