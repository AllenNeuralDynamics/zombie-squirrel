import s3fs
import zarr
import pandas as pd
import numpy as np

s3 = s3fs.S3FileSystem(anon=False)
nwb_dir = "codeocean-s3datasetsbucket-1u41qdg42ur9/6ca96961-867e-48f4-8750-02f0d38c0ff3/nwb/ecephys_800779_2025-09-26_12-57-44_experiment1_recording.nwb"

store = s3fs.S3Map(root=nwb_dir, s3=s3, check=False)
units_group = zarr.open_consolidated(store, mode="r")["units"]

unit_ids = units_group["id"][:]
num_units = len(unit_ids)
print("num_units:", num_units)

spike_times_array = units_group["spike_times"][:]
spike_times_index_array = units_group["spike_times_index"][:]

units_data = {}
for key in sorted(units_group.keys()):
    arr = units_group[key]
    if arr.ndim == 1 and arr.shape[0] == num_units:
        units_data[key] = arr[:].tolist()
    else:
        print("SKIPPING", key, arr.shape)

units_df = pd.DataFrame(units_data)
print("\nunits_df shape:", units_df.shape)

# vectorized spike times construction
counts = np.diff(np.concatenate([[0], spike_times_index_array]))
repeated_unit_ids = np.repeat(unit_ids, counts)
spikes_df = pd.DataFrame({
    "unit_id": repeated_unit_ids,
    "spike_time": spike_times_array,
})
print("spikes_df shape:", spikes_df.shape)
print(spikes_df.head(3))
assert len(spikes_df) == len(spike_times_array), "spike count mismatch"
print("OK")
