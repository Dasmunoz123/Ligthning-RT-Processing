from shapely.errors import WKTReadingError, WKBReadingError
import numpy as np
import binascii
import shapely.speedups
from shapely import wkb, wkt, box
from schemas import discrete_kafka_producer_payload, continuous_kafka_producer_payload
shapely.speedups.enable()


def read_geometry(data):
    try:
        # Try reading as WKT
        return wkt.loads(data)
    except WKTReadingError:
        try:
            # Try reading as WKB
            return wkb.loads(binascii.unhexlify(data))
        except WKBReadingError:
            raise WKTReadingError("Invalid geometry format")


def haversine(lon_a: float, lat_a: float, lon_b: float, lat_b: float) -> float:
    dlon: float = (lon_b - lon_a) * 0.01745323
    dlat = (lat_b - lat_a) * 0.01745323
    ans = (np.sin(dlat * 0.5)**2) + np.cos(lat_a * 0.01745323) * \
        np.cos(lat_b * 0.01745323) * (np.sin(dlon * 0.5)**2)
    return 2 * np.arcsin(np.sqrt(ans)) * 6377.83


def reanalisis(weather, lon_bins, lat_bins, tessellation, grid_boxes, idx, conf):
    # Weather data source (e.g. Linet, GLM, alisios, Ideam)
    phenomena = conf['source']
    source_type = conf['type']  # Weather data type (e.g. discrete, continuos)

    # weather discrete data with specific lat-lon position
    location = read_geometry(data=weather['geom_str'])
    related_weather = []

    # Reanalisis - filter weather data related with assets by geographical proximity
    if source_type == 'discrete':
        lon_id = np.digitize(location.x, lon_bins, right=False)
        lat_id = np.digitize(location.y, lat_bins, right=False)
        box_id = '{}:{}'.format(lon_id, lat_id)

        # Identify if the weather location is within any active tesselation box
        if tessellation.get(box_id) != None:
            assets = tessellation[box_id].assets
            for asset in assets:
                # Check the asset type and configure actions
                reanalisis_data = discrete_reanalisis(asset=asset, location=location, weather=weather,
                                                      conf=conf, phenomena=phenomena, asset_type=asset.type)

                if reanalisis_data is not None:
                    related_weather.extend(reanalisis_data)

    elif source_type == 'continuous':
        weather_box = location.buffer(conf['resolution'] * 0.5).bounds
        lon_weather_box = (weather_box[0], weather_box[2])
        lat_weather_box = (weather_box[1], weather_box[3])

        weather_object = box(
            xmin=min(lon_weather_box),
            ymin=min(lat_weather_box),
            xmax=max(lon_weather_box),
            ymax=max(lat_weather_box)
        )

        assets, assets_ids = [], []
        # To get the tessellation boxes and the assets within
        boxes = [grid_boxes[pos][1] for pos in idx.intersection(weather_box)]
        if boxes:
            for box_id in boxes:
                if tessellation.get(box_id) != None:
                    tesse_assets = tessellation[box_id].assets
                    for x in tesse_assets:
                        if x.id not in assets_ids:
                            assets.append(x)
                            assets_ids.append(x.id)
        if assets:
            for asset in assets:
                reanalisis_data = continous_reanalisis(asset=asset, weather_object=weather_object,
                                                       weather=weather, conf=conf, phenomena=phenomena,
                                                       asset_type=asset.type)

                if reanalisis_data is not None:
                    related_weather.extend(reanalisis_data)

    return related_weather


def discrete_reanalisis(asset, location, weather, conf, phenomena, asset_type):
    if asset_type == 'Point':
        asset_x, asset_y = asset.geometry.x, asset.geometry.y
        dist_km = haversine(lon_a=asset_x, lat_a=asset_y,
                            lon_b=location.x, lat_b=location.y)

    elif asset_type == 'LineString' or asset_type == 'MultiLineString':
        dist_wsg84 = asset.geometry.distance(location)
        # Tranform to kilometers (111 km equal to 1 degree)
        dist_km = dist_wsg84 * (111/1)

    elif asset_type == 'Polygon':
        if asset.geometry.contains(location):
            dist_km = 0
        else:
            dist_wsg84 = asset.geometry.exterior.distance(location)
            dist_km = dist_wsg84 * (111/1)  # Tranform to kilometers

    else:
        exit(code=0)

    if dist_km <= conf['maxdistancekm']:
        dist_km = round(number=dist_km, ndigits=1)
        s = discrete_kafka_producer_payload(
            distance=dist_km, weather=weather, asset=asset, conf=conf, phenomena=phenomena)
    else:
        s = None
    return s


def continous_reanalisis(asset, weather_object, weather, conf, phenomena, asset_type):
    if asset_type == 'Polygon':
        asset_buffer = asset.geometry
    elif asset_type == 'LineString' or asset_type == 'Point' or asset_type == 'MultiLineString':
        asset_buffer = asset.geometry.buffer(conf['maxdistancekm'] * (1/111))
    else:
        exit(code=0)

    # asset and weather object intersection area
    asset_area = asset_buffer.area
    intersection_area = asset_buffer.intersection(weather_object).area
    area_ratio = round(number=intersection_area/asset_area, ndigits=2)

    if area_ratio >= (1/100):
        s = continuous_kafka_producer_payload(
            area_ratio=area_ratio, weather=weather, asset=asset, conf=conf, phenomena=phenomena)
    else:
        s = None
    return s
