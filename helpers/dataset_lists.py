"""Collections of datasets for benchmarking"""

BEAM_GROUP = "gt1l/heights"

# Datasets to read for benchmark tests
ONE_BEAM_GROUP = [
    f"{BEAM_GROUP}/delta_time", 
    f"{BEAM_GROUP}/dist_ph_across",
    f"{BEAM_GROUP}/dist_ph_along",
    f"{BEAM_GROUP}/h_ph",
    f"{BEAM_GROUP}/lat_ph",
    f"{BEAM_GROUP}/lon_ph",
    f"{BEAM_GROUP}/pce_mframe_cnt",
    f"{BEAM_GROUP}/ph_id_channel",
    f"{BEAM_GROUP}/ph_id_count",
    f"{BEAM_GROUP}/ph_id_pulse",
    f"{BEAM_GROUP}/quality_ph",
    f"{BEAM_GROUP}/signal_conf_ph",
]

# Links to h5cloud s3 bucket
# list contents with `aws s3 ls s3://nasa-cryo-scratch/h5cloud/`
S3BUCKET = 's3://nasa-cryo-scratch/h5cloud/'