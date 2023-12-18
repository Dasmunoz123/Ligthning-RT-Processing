# phenomena-agent
The phenomena agent is proposed to act as a filter, stablishing relationships between geometrys and weather data based on geographical distance.

## Elements.

1. Kafka streaming data from weather data (e.g. linet, glm, alisios)
2. Kafka streaming data from assets characteristics (e.g. position, geometry, meteo data flexible subscription)

## Service configuration file

### grid-tessellation:
1. tileside: Side measure in degrees for the square of the grid tessellation. A positive rational number is required.
2. maxlat: Maximum latitude of the influence area.
3. maxlon: Maximum longitude of the influence area.
4. maxlat: Minimum latitude of the influence area.
5. maxlat: Minimum longitude of the influence area.
6. assetbuffer: Radius measure in degrees for the assets influence area and its tessellation boxes asignment. A positive ration numer is required.

### phenomena-agent:
1. source: Source name (e.g. linet, glm, alisios).
2. type: Weather geographical representation type (discrete: For discrete position weather data as ligthning) & (continuous: For weather with discrete position representation but that represents the weather condition for a continuous space).
3. sourcesi: SI units for the weather data.
4. sourcefield: Key for weather data column header.
5. maxdistancekm: Maximum distance for stablish relationship between weather data and the supervised assets. A positive ration numer is required.
6. resolution: For continuous data this is the area radius of the continuous type weather data.   

## Support

|Version|Date|Name|Email|
|-|-|-|-|
|v0.0.1|May 2023|David Sánchez|asanchez@keraunos.co
|||Mateo Restrepo|mrestrepo@keraunos.co