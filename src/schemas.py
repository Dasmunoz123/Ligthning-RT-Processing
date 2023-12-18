import json
import sys
from datetime import datetime
from icecream import ic


def kafka_producer_schema(conf, source_type):
    RELATED_EVENT_SCHEMA = {
        "type": "struct",
        "name": conf + '.Value',
        "fields": [
            {"type": "string", "optional": False, "field": "global_event_id"},
            {"type": "string", "optional": False, "field": "asset_id"},
            {"type": "string", "optional": False, "field": "origin"},
            {"type": "string", "optional": False, "field": "message"},
            {"type": "int32", "optional": False, "field": "organization_id"},
            {"type": "string", "optional": False, "field": "fecha"},
            {"type": "string", "optional": False, "field": "json_data"}
        ]
    }
    return RELATED_EVENT_SCHEMA


def kafka_producer_payload(id_evento, asset_id, phenomena, description, asset_organization, weather_fecha, json_data):
    RELATED_EVENT_PAYLOAD = {
        "global_event_id": id_evento,
        "asset_id": str(asset_id),
        "origin": phenomena,
        "message": description,
        "organization_id": asset_organization,
        "fecha": weather_fecha,
        "json_data": json.dumps(json_data)
    }

    return RELATED_EVENT_PAYLOAD


def discrete_kafka_producer_payload(distance, weather, asset, conf, phenomena):
    # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    # To acumulate data from payloads
    payloads_list = []

    si_units_list = conf['sourcesi']
    si_magnitude_list = conf['sourcefield']
    si_source = conf['source']
    id_evento = weather['id']

    si_tuple = list(zip(si_units_list, si_magnitude_list))

    for tuple in si_tuple:
        si_units, si_magnitude = tuple[0], tuple[1]
        weather_data = weather[si_magnitude]

        if phenomena == 'linet':
            weather_data = round(number=weather_data, ndigits=1)
        elif phenomena == 'glm':
            weather_data = round(number=weather_data * (1/1e-15), ndigits=1)
        else:
            ic(f"The phenomena {phenomena} does not have support, goodbye!")
            exit(code=0)

        json_data = {"distance": distance,
                     "magnitude": weather_data, "units": si_magnitude}

        # Phenomena description depending on asset type
        if asset.type == 'Point' or asset.type == 'LineString':
            description = f"Descarga atmosférica de {weather_data} {si_units} detectada por {si_source} a {distance} km"
        elif asset.type == 'Polygon':
            if distance == 0:
                description = f"Descarga atmosférica de {weather_data} {si_units} detectada por {si_source} en el polígono"
            else:
                description = f"Descarga atmosférica de {weather_data} {si_units} detectada por {si_source} a {distance} km"
        else:
            description = f"Sin descripción disponible"

        payload = kafka_producer_payload(id_evento=id_evento, asset_id=asset.id, phenomena=phenomena,
                                         description=description, asset_organization=asset.organization,
                                         weather_fecha=weather['fecha'], json_data=json_data)

        payloads_list.append(payload)
    return payloads_list


def continuous_kafka_producer_payload(area_ratio, weather, asset, conf, phenomena):
    # To acumulate data from payloads
    payloads_list = []

    si_units_list = conf['sourcesi']
    si_magnitude_list = conf['sourcefield']
    si_source = conf['source']
    id_evento = weather['id']

    si_tuple = list(zip(si_units_list, si_magnitude_list))

    for tuple in si_tuple:
        si_units, si_magnitude = tuple[0], tuple[1]
        weather_data = weather[si_magnitude]

        json_data = {
            "porcentaje": area_ratio,
            "magnitud": weather_data,
            "unidades": si_magnitude
        }
        description = f"Condición atmosférica de {weather_data} {si_units} detectada por {si_source} en el {area_ratio * 100} % del area de influencia"

        payload = kafka_producer_payload(id_evento=id_evento, asset_id=asset.id, phenomena=phenomena,
                                         description=description, asset_organization=asset.organization,
                                         weather_fecha=weather['fecha'], json_data=json_data)

        payloads_list.append(payload)

    return payloads_list
