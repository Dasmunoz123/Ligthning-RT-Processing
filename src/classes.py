import sys
from shapely.geometry import Point, LineString, Polygon, shape
from utils import display_log


class AssetClass:
    def __init__(self, asset):      
        # Create and assign asset class properties
        self.type = asset["geometry"]['type']
        self.geometry = shape(asset['geometry'])
        self.organization = asset['organizationId']  
        self.quality = True
        
        # Get the asset ID (kafka changestream or Mongoclient)
        if type(asset['_id']) == dict:
            self.id = asset['_id']['$oid']
        else:
            self.id = asset['_id']
        
        # Get the asset name
        self.name = asset.get('name','Unknown')
        if self.name == 'Unknown':
            display_log(message=f"Asset ID {self.id} with no name field")


class GridClass:
    def __init__(self, nombre, datos):
        self.name = nombre
        self.assets = datos
