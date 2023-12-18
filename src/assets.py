import numpy as np
from itertools import product
from rtree import index
from classes import AssetClass, GridClass
from icecream import ic
from shapely.geometry import box
import shapely.speedups
shapely.speedups.enable()

def build_grid(conf):
    lat_max = conf['maxlat']
    lat_min = conf['minlat']
    lon_max = conf['maxlon']
    lon_min = conf['minlon']
    width = conf['tileside']
    
    lat_bins = np.arange(start=lat_min, stop=lat_max, step=width, dtype=float)
    lon_bins = np.arange(start=lon_min, stop=lon_max, step=width, dtype=float)

    combinations = np.array(list(product(lon_bins, lat_bins)))
    grid_boxes = []
    for x in combinations:
        bbox = box(x[0], x[1], x[0] + width, x[1] + width)
        lon_id = np.digitize(x[0], lon_bins, right=False)
        lat_id = np.digitize(x[1], lat_bins, right=False)
        bbox_id = '{}:{}'.format(lon_id,lat_id)
        grid_boxes.append([bbox,bbox_id])

    return lat_bins, lon_bins, grid_boxes


def build_assets(assets_list):
    ## To filter assets to supervise and build assets as objects
    assets_to_supervise = []
    if type(assets_list) == list:
        for item in assets_list:
            new_object = AssetClass(asset=item)
            if new_object.quality == True:
                assets_to_supervise.append(new_object)
    else:
        new_object = AssetClass(asset=assets_list)
        if new_object.quality == True:
            assets_to_supervise.append(new_object)
    
    return assets_to_supervise


def addto_tessellation(tessellation, grid_boxes, idx, assets_list, assets_dict, assets_buffer):
    
    for asset in assets_list: # Loop through each Shapely geometry
        boxes = [grid_boxes[pos][1] for pos in idx.intersection(asset.geometry.envelope.buffer(assets_buffer).bounds)]
        if boxes:
            for box in boxes:
                if tessellation.get(box) == None:
                    tessellation[box] = GridClass(nombre=box, datos=[asset])
                else:
                    tessellation[box].assets.append(asset)
            assets_dict[asset.id] = boxes                                       ## Adding relevant to the assets dict
        else:
            message = f"Asset {asset.id} does not fit into the interest area"
            ic(message)
    
    return tessellation, assets_dict


def build_tessellation(conf, assets_list):
    
    ## Generates a grid within the desired area to monitoring
    lat_bins, lon_bins, grid_boxes = build_grid(conf=conf)

    ## Make a list with all the monitored assets
    assets = build_assets(assets_list=assets_list)

    ## Relate the assets and the grid in a tesselation dictionary
    idx = index.Index()
    tessellation = dict()
    assets_dict = dict()
    for pos, cell in enumerate(iterable=grid_boxes):            # Populate R-tree index with bounds of grid cells
        idx.insert(id=pos, coordinates=cell[0].bounds)          # assuming cell is a shapely object
    
    tessellation, assets_dict = addto_tessellation(tessellation=tessellation, grid_boxes=grid_boxes, idx=idx, assets_list=assets,
                                                   assets_dict=assets_dict, assets_buffer=conf['assetbuffer'])
    return [lat_bins, lon_bins, grid_boxes, idx, tessellation, assets_dict]


def upsert_tessellation(tessellation, grid_boxes, idx, assets, assets_dict, conf):
    assets = build_assets(assets_list=assets)
    tessellation, assets_dict = addto_tessellation(tessellation=tessellation, grid_boxes=grid_boxes, idx=idx, assets_list=assets, 
                                                   assets_dict=assets_dict, assets_buffer=conf['assetbuffer'])
    return tessellation, assets_dict


def drop_tessellation(tessellation, assets_dict, asset_id):
    latest_boxes = assets_dict[asset_id]
    for latest_box in latest_boxes:
        for element in tessellation[latest_box].assets:
            if element.id == asset_id:
                tessellation[latest_box].assets.remove(element)
    
    assets_dict.pop(asset_id, None)
    
    return tessellation, assets_dict