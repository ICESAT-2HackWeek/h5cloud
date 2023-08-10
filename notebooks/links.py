import s3fs

import json
from pathlib import Path

S3PATH = s3path = f"s3://nasa-cryo-scratch/h5cloud/*"

S3FILELINKS = Path("../s3filelinks.json")

class S3Links:
    """Class to load and return S3 links for test files
    
    This loads s3filelinks.json as a dictionary.
    
    # Import S3Links class
    from links import S3Links
    
    # Instantiate class
    s3links = S3Links()
    
    # Get a list of available formats
    s3links.formats
    ['h5repack', 'original']
    
    # Get a list of s3 links for a format
    s3links.get_links_by_format('h5repack')
    
    ['h5cloud/h5repack/ATL03_20181120182818_08110112_006_02_repacked.h5',
     'h5cloud/h5repack/ATL03_20190219140808_08110212_006_02_repacked.h5',
     'h5cloud/h5repack/ATL03_20200217204710_08110612_006_01_repacked.h5',
     'h5cloud/h5repack/ATL03_20211114142614_08111312_006_01_repacked.h5',
     'h5cloud/h5repack/ATL03_20230211164520_08111812_006_01_repacked.h5']
     
    # Get a S3 link by filename
    s3links.get_link_by_name('ATL03_20181120182818_08110112_006_02_repacked.h5')
    'h5cloud/h5repack/ATL03_20181120182818_08110112_006_02_repacked.h5'
    
    # Get a S3 link for a format by index
    s3links.get_link_by_fileid('original', 0)
    'h5cloud/original/ATL03_20181120182818_08110112_006_02.h5'
    """
    
    def __init__(self, source="from_bucket"):
        self.json_file = S3FILELINKS
        self.table = load_s3testfile(S3FILELINKS)
        self.formats = list(self.table.keys())
        
    
    def get_links_by_format(self, file_format):
        """Returns a list of links for a given format
        
        See self.formats to get a list of formats
        """
        try:
            return list(self.table[file_format].values())
        except KeyError:
            print(f"Unknown file_format.  Expects one of {self.formats}")
            return None
        
        
    def get_link_by_name(self, name):
        """Gets a S3 link for a given filename"""
        try:
            return self.dict_by_name()[name]
        except:
            print(f"{name} not found in self.table")
            return None
        
    def get_link_by_fileid(self, file_format: str, fileid: int)-> str:
        """Returns an S3 link for a file_format and fileid - array index 
        
        file_format : one of the file formats listed in self.formats
        fileid : array index for file
        
        returns : s3 link as string
        """
        return self.get_links_by_format(file_format)[fileid]
      
    def dict_by_name(self):
        """Returns a dictionary of all files indexed by name"""
        return dict([make_entry(link) for fmt in self.table.values() for link in fmt.values()])
    
    
def load_s3testfile(file_format):
    """Loads json file with s3 links"""
    with open(S3FILELINKS, 'r') as f:
        json_obj = json.load(f)
    return json_obj


def glob_s3testfile():
    """Generates dict of testfiles using glob of s3 bucket"""
    filelist = {}
    for folder in s3.glob(S3PATH):
        file_format = folder.split('/')[-1]
        if file_format not in filelist.keys():
            filelist[file_format] = {}
        for path in s3.glob(f"{folder}/*"):
            name = path.split('/')[-1]
            filelist[file_format][name] = path
    return filelist

            
def make_entry(path):
    """Helper to return tuple containing a filename and s3 link"""
    name = path.split('/')[-1]
    return name, path