#!/usr/bin/env node

import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const DEFAULT_SOURCE_URL =
  'https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_roads.geojson';
const DEFAULT_RELATIVE_DEST = 'public/data/roads-major.geojson';
const DEFAULT_KEEP_TYPES = new Set(['major highway', 'beltway', 'bypass']);

function printUsage() {
  console.log(`Usage: node scripts/build-major-roads.mjs [options]

Options:
  --source <url>           Source GeoJSON URL
  --dest <path>            Destination file path
  --include-secondary      Include "Secondary Highway"
  --round <digits>         Coordinate rounding digits (default: 5)
  --help                   Show this message
`);
}

function parseArgs(argv) {
  const args = {
    source: DEFAULT_SOURCE_URL,
    dest: path.join(process.cwd(), DEFAULT_RELATIVE_DEST),
    includeSecondary: false,
    roundDigits: 5,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--help' || token === '-h') {
      args.help = true;
      continue;
    }
    if (token === '--source') {
      args.source = argv[index + 1] ?? '';
      index += 1;
      continue;
    }
    if (token === '--dest') {
      const next = argv[index + 1] ?? '';
      args.dest = path.isAbsolute(next) ? next : path.join(process.cwd(), next);
      index += 1;
      continue;
    }
    if (token === '--include-secondary') {
      args.includeSecondary = true;
      continue;
    }
    if (token === '--round') {
      const value = Number.parseInt(argv[index + 1] ?? '', 10);
      if (Number.isFinite(value)) {
        args.roundDigits = Math.max(0, Math.min(8, value));
      }
      index += 1;
      continue;
    }
    throw new Error(`Unknown option: ${token}`);
  }

  if (!args.source) {
    throw new Error('Missing --source URL');
  }
  return args;
}

function normalizeRoadClass(feature) {
  const properties = feature?.properties ?? {};
  const candidates = [
    properties.type,
    properties.class,
    properties.highway,
    properties.featurecla,
  ];
  for (const value of candidates) {
    if (typeof value !== 'string') {
      continue;
    }
    const normalized = value.trim().toLowerCase();
    if (normalized) {
      return normalized;
    }
  }
  return '';
}

function roundNumber(value, digits) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function roundLineStringCoordinates(rawCoordinates, digits) {
  if (!Array.isArray(rawCoordinates)) {
    return [];
  }
  const points = [];
  for (const coordinate of rawCoordinates) {
    if (!Array.isArray(coordinate) || coordinate.length < 2) {
      continue;
    }
    const lon = Number(coordinate[0]);
    const lat = Number(coordinate[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
      continue;
    }
    const rounded = [roundNumber(lon, digits), roundNumber(lat, digits)];
    const last = points[points.length - 1];
    if (!last || last[0] !== rounded[0] || last[1] !== rounded[1]) {
      points.push(rounded);
    }
  }
  return points;
}

function normalizeGeometry(geometry, digits) {
  if (!geometry || typeof geometry !== 'object') {
    return null;
  }
  if (geometry.type === 'LineString') {
    const points = roundLineStringCoordinates(geometry.coordinates, digits);
    if (points.length < 2) {
      return null;
    }
    return { type: 'LineString', coordinates: points };
  }
  if (geometry.type === 'MultiLineString') {
    const lines = Array.isArray(geometry.coordinates)
      ? geometry.coordinates
          .map((line) => roundLineStringCoordinates(line, digits))
          .filter((line) => line.length >= 2)
      : [];
    if (lines.length === 0) {
      return null;
    }
    return { type: 'MultiLineString', coordinates: lines };
  }
  return null;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return;
  }

  const keepTypes = new Set(DEFAULT_KEEP_TYPES);
  if (args.includeSecondary) {
    keepTypes.add('secondary highway');
  }

  console.log(`[roads] Fetching source: ${args.source}`);
  const response = await fetch(args.source);
  if (!response.ok) {
    throw new Error(`Source fetch failed: ${response.status} ${response.statusText}`);
  }
  const sourceJson = await response.json();
  if (!sourceJson || sourceJson.type !== 'FeatureCollection' || !Array.isArray(sourceJson.features)) {
    throw new Error('Source file is not a valid GeoJSON FeatureCollection');
  }

  let scanned = 0;
  let kept = 0;
  let skippedClass = 0;
  let skippedGeometry = 0;
  const outputFeatures = [];

  for (const feature of sourceJson.features) {
    scanned += 1;
    const roadClass = normalizeRoadClass(feature);
    if (!keepTypes.has(roadClass)) {
      skippedClass += 1;
      continue;
    }

    const geometry = normalizeGeometry(feature.geometry, args.roundDigits);
    if (!geometry) {
      skippedGeometry += 1;
      continue;
    }

    kept += 1;
    const properties = feature.properties ?? {};
    outputFeatures.push({
      type: 'Feature',
      properties: {
        class: roadClass,
        country: typeof properties.sov_a3 === 'string' ? properties.sov_a3 : '',
        name: typeof properties.name === 'string' ? properties.name : '',
        min_zoom: Number.isFinite(properties.min_zoom) ? Number(properties.min_zoom) : null,
        length_km: Number.isFinite(properties.length_km) ? Number(properties.length_km) : null,
      },
      geometry,
    });
  }

  const output = {
    type: 'FeatureCollection',
    generatedAt: new Date().toISOString(),
    source: args.source,
    keepTypes: Array.from(keepTypes).sort(),
    featureCount: outputFeatures.length,
    features: outputFeatures,
  };

  await fs.mkdir(path.dirname(args.dest), { recursive: true });
  const payload = `${JSON.stringify(output)}\n`;
  await fs.writeFile(args.dest, payload, 'utf8');

  console.log(
    `[roads] Wrote ${args.dest} (${Buffer.byteLength(payload)} bytes)`
  );
  console.log(
    `[roads] scanned=${scanned} kept=${kept} skipped_class=${skippedClass} skipped_geometry=${skippedGeometry}`
  );
}

main().catch((error) => {
  console.error(`[roads] ERROR: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
