"""Collections of datasets for benchmarhing"""

BEAM_GROUP = "gt1l/land_ice_segments"

# Datasets to read for benchmark tests
ONE_BEAM_GROUP = [
    f"{BEAM_GROUP}/atl06_quality_summary", 
    f"{BEAM_GROUP}/delta_time",
    f"{BEAM_GROUP}/h_li",
    f"{BEAM_GROUP}/h_li_sigma",
    f"{BEAM_GROUP}/latitude",
    f"{BEAM_GROUP}/longitude",
    f"{BEAM_GROUP}/segment_id",
    f"{BEAM_GROUP}/sigma_geo_h",
]

XARRAY_LIKE_ONE_DATASET = [
    f"{BEAM_GROUP}/delta_time",
    f"{BEAM_GROUP}/h_li",
    f"{BEAM_GROUP}/latitude",
    f"{BEAM_GROUP}/longitude",
]

# Links to h5cloud s3 bucket
# list contents with `aws s3 ls s3://nasa-cryo-scratch/h5cloud/`
S3BUCKET = 's3://nasa-cryo-scratch/h5cloud/'