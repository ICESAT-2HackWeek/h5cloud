import yaml
from pydantic import Field, BaseModel
from typing import Dict, Optional

class FileConfig(BaseModel):
    link: str
    processing: Optional[str] = None

class TestConfig(BaseModel):
    results_bucket: str
    results_directory: str
    collection: str
    group: str
    variable: str
    lat_group: Optional[str] = None
    lon_group: Optional[str] = None
    files: Dict[str, FileConfig]
        
    def load_from_yaml(yaml_file: str, results_bucket: str, results_directory: str):
        """
        Load the YAML configuration from the specified file and create a TestCollectionFiles object.
        """
        try:
            with open(yaml_file, 'r') as file:
                data = yaml.safe_load(file)
                return TestConfig(results_bucket=results_bucket, results_directory=results_directory, **data)
        except FileNotFoundError:
            print(f"Error: The file {self.yaml_file} was not found.")
        except yaml.YAMLError as exc:
            print(f"Error in YAML formatting: {exc}")
        except Exception as e:
            print(f"An error occurred: {e}")

        return None