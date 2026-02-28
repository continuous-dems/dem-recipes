#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import yaml
import logging
import subprocess
from fetchez.spatial import Region
try:
    from fetchez.pipeline import Recipe
except ModuleNotFoundError:
    Recipe = None

# Configuration
GEOJSON_PATH = "crm_vol6_south.geojson"
TEMPLATE_PATH = "crm_vol6_config.yaml"
OUTPUT_DIR = "./crm_vol6_output_tiles"

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SoCal_Builder")

def build_tile(feature, template_str):
    """Builds a single CRM tile."""

    # Extract bounds/metadata from GEOJSON_PATH
    coords = feature["geometry"]["coordinates"][0]
    xs = [pt[0] for pt in coords]
    ys = [pt[1] for pt in coords]
    w, e, s, n = min(xs), max(xs), min(ys), max(ys)
    region = Region(w, e, s, n)

    # Extract tile metadata
    props = feature.get("properties", {})
    tile_name = props.get("NAME") or props.get("ID") or None
    if tile_name is None:
        tile_name = region.format("fn")

    logger.info(f"--- STARTING TILE: {tile_name} [{w:.2f}, {e:.2f}, {s:.2f}, {n:.2f}] ---")

    # Create tile directory
    tile_dir = os.path.join(OUTPUT_DIR, tile_name)
    os.makedirs(tile_dir, exist_ok=True)

    # Move into tile directory so we contain processing
    original_cwd = os.getcwd()
    os.chdir(tile_dir)

    try:
        # Fill template
        config_str = template_str.format(
            name=tile_name,
            w=w, e=e, s=s, n=n
        )
        tile_config_fn = f"socal_{tile_name}.yaml"
        with open(tile_config_fn, 'w') as f:
            f.write(config_str)

        logger.info(f"Saved configuration to: {tile_config_fn}")

        config = yaml.safe_load(config_str)

        if Recipe is not None:
            try:
                recipe = Recipe(config)
                recipe.run()
            except Exception:
                subprocess.run(["fetchez", tile_config_fn], check=True)
        else:
            subprocess.run(["fetchez", tile_config_fn], check=True)

        logger.info(f"--- FINISHED TILE: {tile_name} ---")

    except Exception as e:
        logger.error(f"FAILED TILE {tile_name}: {e}")

    finally:
        # Return to root
        os.chdir(original_cwd)

def main():
    # Load template
    with open(TEMPLATE_PATH, 'r') as f:
        template_str = f.read()

    # Load GeoJSON
    with open(GEOJSON_PATH, 'r') as f:
        geojson = json.load(f)

    features = geojson.get("features", [])
    logger.info(f"Found {len(features)} tiles to process.")

    # Process sequentially (or use multiprocessing.Pool for parallel maybe?)
    for feature in features:
        build_tile(feature, template_str)

if __name__ == "__main__":
    main()
