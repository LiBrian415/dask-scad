"""
Original: https://www.kaggle.com/kartikkannapur/nyc-taxi-trips-exploratory-data-analysis/notebook

Changes:
 - Use dask instead of pandas
 - Removed plots with equivalent dataframe queries
 - Only analyze a single dataset
"""

import dask.dataframe as dd

from haversine import haversine, Unit

data_path = "input/data.csv"

# Datasets
df = dd.read_csv(data_path)

print("*** Dataset ***")
print(df.compute())
print()
print()

# Dataset Dimensions
print("*** Basic Analysis ***")
print("Total number of samples: ", df.shape[0].compute())
print("Number of columns: ", df.shape[1])
print()

print("head():")
print(df.head())
print()

print("describe():")
print(df.describe().compute())
print()

print("info():")
print(df.info())
print()

print("Null Count:")
print(df.isnull().sum().compute())
print()

print("Duplicate Check:")
print("Number of ids: ", len(df["id"]))
print("Number of unique ids: ", len(df["id"].drop_duplicates().compute()))
print()
print()

print("*** Vendor Analysis ***")
unique_vendors = df["vendor_id"].drop_duplicates().compute()
print("Number of vendor_ids: ", len(df["vendor_id"]))
print("Number of unique vendor_ids: ", len(unique_vendors))
print()

print("Occurrences of vendor_ids:")
print(df["vendor_id"].value_counts().compute())
print()

print("Vendor-Passenger count distribution:")
vendors_list = unique_vendors.tolist()
vendors_list.sort()
for v in vendors_list:
    print("vendor_id: ", v)
    print(df[df["vendor_id"] == v]["passenger_count"].value_counts(sort=False).compute())
    print()


print("*** Datetime Analysis ***")
df["pickup_datetime"] = dd.to_datetime(df["pickup_datetime"])
df["dropoff_datetime"] = dd.to_datetime(df["dropoff_datetime"])

print("Trip Duration (Dataset):")
print(df["trip_duration"].describe().compute())
print()

print("Trip Duration (Computed):")
print((df["dropoff_datetime"] - df["pickup_datetime"]).describe().compute())
print()

print("Trip Duration (Exclude Outliers [longer than 5 days])")
df = df[df["trip_duration"] < 500000]
print((df["dropoff_datetime"] - df["pickup_datetime"]).describe().compute())
print()
print()


print("*** Store and Forward Analysis ***")
print("store_and_fwd_flag count distribution:")
print(df["store_and_fwd_flag"].value_counts(sort=False).compute())
print()

print("Percentage that didn't store and forward:")
print(len(df[df["store_and_fwd_flag"] == "N"].compute())*100.0/(df.count().compute()[0]))
print()

print("Vendors that needed to store and forward:")
print(set(df[df["store_and_fwd_flag"] == "Y"]["vendor_id"].compute()))
print()
print()


print("*** Haversine Analysis ***")
def calculate_haversine_distance(var_row):
    return haversine((var_row["pickup_latitude"], var_row["pickup_longitude"]),
                     (var_row["dropoff_latitude"], var_row["dropoff_longitude"]), unit=Unit.MILES)


df["haversine_distance"] = df.apply(lambda row: calculate_haversine_distance(row), axis=1, meta=(None, 'float64'))

print("Validate distances:")
print(df.head())
print()

print("Haversine Distance:")
print(df["haversine_distance"].describe().compute())
print()

print("Remove Outliers:")
print(df[df["haversine_distance"] > 100].compute())
print()
print()


print("*** Temporal Analysis ***")
print("Train dataset start date: ", min(df["pickup_datetime"].compute()))
print("Train dataset end date: ", max(df["pickup_datetime"].compute()))
print()

df["pickup_dayofweek"] = df.pickup_datetime.dt.dayofweek
df["pickup_weekday_name"] = df.pickup_datetime.dt.day_name()
df["pickup_hour"] = df.pickup_datetime.dt.hour
df["pickup_month"] = df.pickup_datetime.dt.month

print("Validate Temporal Elements:")
print(df.head())
print()

print("Ride distribution across days:")
print(df["pickup_weekday_name"].value_counts(sort=False).compute())
print()

print("Ride distribution across hours:")
print(df["pickup_hour"].value_counts(sort=False).compute())
print()

print("Ride distribution across months:")
print(df["pickup_month"].value_counts(sort=False).compute())
print()

print("Trip Duration Stats:")
print(df.trip_duration.describe().compute())
print()

print("Median trip duration across days:")
df_train_agg = df.groupby('pickup_weekday_name')['trip_duration'].apply(lambda x: x.quantile(),
                                                                        meta=(None, 'float64')).compute()
print(df_train_agg)
print()

print("Trip duration stats across days:")
# Compute meta by performing same computation on sample pandas DataFrame
# Ref: https://github.com/dask/dask/issues/8624
meta = df.head().groupby('pickup_weekday_name')['trip_duration'].apply(lambda x: x.describe())
print(df.groupby('pickup_weekday_name')['trip_duration'].apply(lambda x: x.describe(),
                                                               meta=meta).compute())
print()

print("Median trip duration across hours:")
df_train_agg = df.groupby('pickup_hour')['trip_duration'].apply(lambda x: x.quantile(),
                                                                meta=(None, 'float64')).compute()
print(df_train_agg)
print()

print("Trip duration stats across hours:")
meta = df.head().groupby('pickup_hour')['trip_duration'].apply(lambda x: x.describe())
print(df.groupby('pickup_hour')['trip_duration'].apply(lambda x: x.describe(),
                                                       meta=meta).compute())
print()

print("Median trip duration across months:")
df_train_agg = df.groupby('pickup_month')['trip_duration'].apply(lambda x: x.quantile(),
                                                                 meta=(None, 'float64')).compute()
print(df_train_agg)
print()

print("Trip duration stats across months:")
meta = df.head().groupby('pickup_month')['trip_duration'].apply(lambda x: x.describe())
print(df.groupby('pickup_month')['trip_duration'].apply(lambda x: x.describe(),
                                                        meta=meta).compute())
print()
