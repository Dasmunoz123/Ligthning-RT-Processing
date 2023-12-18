# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.3] - 2023-11-22 - Andrés Osorio <aosorio@keraunos.co>

-Se actualiza la version base de debian a slim-bookworm

## [0.0.2] - 2023-11-22 - Andrés Osorio <aosorio@keraunos.co>

- Ahora el agente por fenomenos puede relacionar eventos a MultiLineString. Además, la geometría de los eventos puede ser WKT o WKB.

## [0.0.1] - 2023-10-12 - David Sanchez <asanchez@keraunos.co>

### Added

- First approach to phenomena-agent for discrete and continuous meteorological events. Phenomena-agent purpose is to filter and calculate relationships between assets and meteorological events (i.e. strokes, precipitation, radiation)
- Read meteorological data from kafka topic in real time
- Read assets from MongoDB collections (Supports assets with point, line and polygon geometries)


[unreleased]: https://github.com/k-clarity/phenomena-agent/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/k-clarity/phenomena-agent/releases/tag/v0.0.1