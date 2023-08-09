import json
from pathlib import Path

S3FILELINKS = Path("../s3filelinks.json")

class S3Links:
    """Class to load and return S3 links for test files
    
    This loads s3filelinks.json as a dictionary.
    
    s3links = S3Links()
    
    """
    
    def __init__(self):
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
        
    def get_link_by_fileid(self, file_format -> str, fileid -> int) -> str:
        """Returns an S3 link for a file_format and fileid - array index 
        
        file_format : one of the file formats listed in self.formats
        fileid : array index for file
        
        returns : s3 link as string
        """
        return self.get_links_by_format(file_format)[fileid]
      
    def dict_by_name(self):
        """Returns a dictionary of all files indexed by name"""
        return dict([make_entry(link) for fmt in self.table.values() for link in fmt.values()])
    
    
def load_s3testfile(file_format, filename=None, fileid=None):
    """Loads json file with s3 links"""
    with open(S3FILELINKS, 'r') as f:
        json_obj = json.load(f)
    return json_obj

def make_entry(path):
    """Helper to return tuple containing a filename and s3 link"""
    name = path.split('/')[-1]
    return name, path