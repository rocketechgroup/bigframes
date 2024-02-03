import os
import bigframes.pandas as bpd

bpd.options.bigquery.project = os.environ.get("GCP_PROJECT_ID")
bpd.options.bigquery.location = "eu"


# Warning, all remote functions created via bigframes has a public ingress rule,
# currently there appears to be no way to change it at creation time
# see https://github.com/googleapis/python-bigquery-dataframes/blob/main/bigframes/remote_function.py#L404


@bpd.remote_function(
    [int],
    str,
    bigquery_connection="bigframes-rf-conn",
    reuse=True,
    packages=[],
)
def get_mapped_duration(x):
    if x < 120:
        return "under 2 hours"
    elif x < 240:
        return "2-4 hours"
    elif x < 360:
        return "4-6 hours"
    elif x < 480:
        return "6-8 hours"
    elif 480 <= x < 1440:
        return "8-24 hours"
    else:
        return "> 24 hours"


@bpd.remote_function(
    [str],
    str,
    bigquery_connection="bigframes-rf-conn",
    reuse=True,
    packages=["cryptography"],
)
def get_encrypted(x):
    from cryptography.fernet import Fernet

    # handle missing value
    if x is None:
        x = ""
    else:
        x = str(x)

    key = Fernet.generate_key()
    f = Fernet(key)
    return f.encrypt(x.encode()).decode()


df = bpd.read_gbq(
    """
WITH all_clean as (
  SELECT
  date(start_date) as start_date,
  date(end_date) as end_date,
  EXTRACT(DAYOFWEEK from DATE(start_date)) AS day_of_week,
  start_station_id,
  start_station_name,
  duration,
  count(*) as transactions
FROM
  `bigquery-public-data.london_bicycles.cycle_hire`
WHERE
  start_date is not null
  and end_date is not null
  and start_station_id is not null
  and duration is not null
GROUP BY
  1,2,3,4,5,6
)
SELECT * FROM all_clean
WHERE start_date > '2023-01-01'
"""
)

# get_encrypted = bpd.read_gbq_function('<name of the remote function>')
# get_mapped_duration = bpd.read_gbq_function('<name of the remote function>')

df["day_of_week"] = df["day_of_week"].map(
    {
        2: "Monday",
        3: "Tuesday",
        4: "Wednesday",
        5: "Thursday",
        6: "Friday",
        7: "Saturday",
        1: "Sunday",
    }
)

df["duration"] = df["duration"].map(get_mapped_duration)
df["start_station_name"] = df["start_station_name"].map(get_encrypted)

top_5_by_day_of_week = (
    df.groupby(["day_of_week", "duration"])
    .agg({"transactions": "sum"})
    .reset_index()
    .sort_values(by="transactions", ascending=False)
    .head(5)
)

top_5_addresses = (
    df.groupby(["start_station_name"])
    .agg({"transactions": "sum"})
    .reset_index()
    .sort_values(by="transactions", ascending=False)
    .head(5)
)

print(top_5_by_day_of_week)
print(top_5_addresses)


# Clean up cloud artifacts
# session = bpd.get_global_session()
# for function in (get_mapped_duration, get_encrypted):
#     try:
#         session.bqclient.delete_routine(function.bigframes_remote_function)
#     except Exception:
#         # Ignore exception during clean-up
#         pass
#
#     try:
#         session.cloudfunctionsclient.delete_function(
#             name=function.bigframes_cloud_function
#         )
#     except Exception:
#         # Ignore exception during clean-up
#         pass
