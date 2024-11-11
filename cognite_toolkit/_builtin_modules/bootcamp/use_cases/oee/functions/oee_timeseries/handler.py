from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, Dict

import numpy as np
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteNotFoundError
from retry import retry


def handle(client: CogniteClient, data: Dict[str, Any] = {}) -> None:
    lookback_minutes = timedelta(minutes=data.get("lookback_minutes", 60)).total_seconds() * 1000
    data_set_external_id = data.get("data_set_external_id", "ds_uc_oee")
    data_set_id = client.data_sets.retrieve(external_id=data_set_external_id).id
    all_sites = [
        "Oslo",
        "Houston",
        "Kuala_Lumpur",
        "Hannover",
        "Nuremberg",
        "Marseille",
        "Sao_Paulo",
        "Chicago",
        "Rotterdam",
        "London",
    ]
    sites = data.get("sites", all_sites)

    print(f"Processing datapoints for these sites: {sites}")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_site, client, data_set_id, lookback_minutes, site) for site in sites]
        for f in futures:
            f.result()


@retry(tries=5, delay=5)
def process_site(client, data_set_id, lookback_minutes, site):
    assets = client.assets.list(asset_subtree_external_ids=site.lower(), limit=None)
    timeseries = client.time_series.list(asset_subtree_external_ids=site.lower(), limit=None)
    external_ids = [ts.external_id for ts in timeseries]
    all_latest_dps = client.time_series.data.retrieve_latest(external_id=external_ids)

    # Organize latest datapoints by equipment for alignment
    assets_dps = {
        asset.external_id: [latest_dp for latest_dp in all_latest_dps if asset.external_id in latest_dp.external_id]
        for asset in assets
    }

    for asset, latest_dps in assets_dps.items():
        end = max([dp.timestamp[0] for dp in latest_dps if latest_dps and dp.timestamp], default=None)
        if end:
            dps_df = client.time_series.data.retrieve_dataframe(
                external_id=[dp.external_id for dp in latest_dps],
                start=end - lookback_minutes,
                end=end,
                aggregates=["sum"],
                granularity="1m",
                include_aggregate_name=False,
                limit=None,
            )

            # Frontfill because "planned_status" and "status" only have datapoints when the value changes
            dps_df = dps_df.ffill()

            # Fill the rest with the opposite
            try:
                first_valid_value = dps_df[f"{asset}:planned_status"].loc[
                    dps_df[f"{asset}:planned_status"].first_valid_index()
                ]
            except:
                print(f"Failed to find datapoints for {asset}:planned_status")
                continue
            backfill_value = 1.0 if first_valid_value == 0.0 else 0.0
            dps_df[f"{asset}:planned_status"] = dps_df[f"{asset}:planned_status"].fillna(value=backfill_value)

            # Same for status
            first_valid_value = dps_df[f"{asset}:status"].loc[dps_df[f"{asset}:status"].first_valid_index()]
            backfill_value = 1.0 if first_valid_value == 0.0 else 0.0
            dps_df[f"{asset}:status"] = dps_df[f"{asset}:status"].fillna(value=backfill_value)

            count_dps = dps_df[f"{asset}:count"]
            good_dps = dps_df[f"{asset}:good"]
            status_dps = dps_df[f"{asset}:status"]
            planned_status_dps = dps_df[f"{asset}:planned_status"]

            total_items = len(count_dps)

            if total_items != len(good_dps) or total_items != len(status_dps) or total_items != len(planned_status_dps):
                # We expect ALL dependent timeseries to have the exact same number of datapoints
                # for the specified time range for the calculation to execute.
                print(
                    f"""{asset}: Unable to retrieve datapoints for all required OEE timeseries (count, good, status, planned_status)
                    between {end - lookback_minutes} and {end}. Ensure that data is available for the time range specified."""
                )

            # Calculate the components of OEE
            dps_df[f"{asset}:off_spec"] = count_dps - good_dps
            dps_df[f"{asset}:quality"] = good_dps / count_dps
            dps_df[f"{asset}:performance"] = (count_dps / status_dps) / (60.0 / 3.0)
            dps_df[f"{asset}:availability"] = status_dps / planned_status_dps

            dps_df[f"{asset}:oee"] = (
                dps_df[f"{asset}:quality"] * dps_df[f"{asset}:performance"] * dps_df[f"{asset}:availability"]
            )

            # Fill in the divide by zeros
            dps_df = dps_df.fillna(value=0.0)
            dps_df = dps_df.replace([np.inf, -np.inf], 0.0)

            # Drop input timeseries
            dps_df = dps_df.drop(
                columns=[f"{asset}:{postfix}" for postfix in ["good", "count", "status", "planned_status"]]
            )

            try:
                client.time_series.data.insert_dataframe(dps_df)
            except CogniteNotFoundError as e:
                # Create the missing oee timeseries since they don't exist
                ts_to_create = []
                for ext_id in e.failed:
                    external_id = ext_id["externalId"]

                    # change external_id to a readable name
                    # Ex: "OSLPROFILTRASYS185:off_spec" to "OSLPROFILTRASYS185 Off Spec"
                    name = external_id.split(":")
                    typ = name[-1]
                    name[-1] = name[-1].replace("_", " ").title()

                    ts_to_create.append(
                        TimeSeries(
                            external_id=external_id,
                            name=" ".join(name),
                            metadata={"type": typ},
                            data_set_id=data_set_id,
                        )
                    )

                client.time_series.create(ts_to_create)
                client.time_series.data.insert_dataframe(dps_df)

            print(f"    {asset} Finished")
