import json
from pprint import pprint

filelist = """2023-08-09 05:30:04 7760000000 h5cloud/h5repack/ATL03_20181120182818_08110112_006_02_repacked.h5
2023-08-09 05:30:04 7008000000 h5cloud/h5repack/ATL03_20190219140808_08110212_006_02_repacked.h5
2023-08-09 05:30:04 6936000000 h5cloud/h5repack/ATL03_20200217204710_08110612_006_01_repacked.h5
2023-08-09 05:30:04 8400000000 h5cloud/h5repack/ATL03_20211114142614_08111312_006_01_repacked.h5
2023-08-09 05:30:04 7960000000 h5cloud/h5repack/ATL03_20230211164520_08111812_006_01_repacked.h5
2023-08-08 23:45:34 7754735138 h5cloud/original/ATL03_20181120182818_08110112_006_02.h5
2023-08-08 23:47:04 6997123664 h5cloud/original/ATL03_20190219140808_08110212_006_02.h5
2023-08-08 23:47:04 6925710500 h5cloud/original/ATL03_20200217204710_08110612_006_01.h5
2023-08-08 23:47:04 8392279594 h5cloud/original/ATL03_20211114142614_08111312_006_01.h5
2023-08-08 23:47:04 7954039827 h5cloud/original/ATL03_20230211164520_08111812_006_01.h5"""

fileformat = {}
for file in filelist.split('\n'):
    path = file.split()[-1]
    _, format_type, dataset = path.split('/')
    if format_type in fileformat:
        fileformat[format_type][dataset] = path
    else:
        fileformat[format_type] = {}
        fileformat[format_type][dataset] = path

json_obj = json.dumps(fileformat)

with open('s3filelinks.json', 'w') as f:
    f.write(json_obj)
