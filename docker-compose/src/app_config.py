# Wrapper Class
import json
from typing import List, Dict, Any, Optional

class AppConfig:
    def __init__(self, config_file: str):
        with open(config_file, "r") as f:
            self.config = json.load(f)

        # Preprocess into structured dict
        self.data = {}
        for app_name, layers in self.config.items():
            self.data[app_name] = {}
            for layer_obj in layers:  # each is {"Staging": {...}}, {"Bronze": {...}}, ...
                for layer_name, details in layer_obj.items():
                    self.data[app_name][layer_name] = details

    def get_applications(self) -> str:
        """Return name of application."""
        return list(self.data.keys())[0]

    def get_layers(self, app: str) -> List[str]:
        """Return list of layers (Staging, Bronze, Silver, Gold)."""
        return list(self.data.get(app, {}).keys())

    def get_database_name(self, app: str, layer: str) -> Optional[str]:
        """Return database name for given application + layer."""
        return self.data.get(app, {}).get(layer, {}).get("Database_Name")

    # ---------- Staging Specific ----------
    def get_files(self, app: str) -> List[Dict[str, Any]]:
        """Return list of file sources (only for Staging)."""
        return self.data.get(app, {}).get("Staging", {}).get("Source", {}).get("File", [])

    def get_databases(self, app: str) -> List[Dict[str, Any]]:
        """Return list of database sources (only for Staging)."""
        return self.data.get(app, {}).get("Staging", {}).get("Source", {}).get("Database", [])

    # ---------- Bronze/Silver/Gold ----------
    def get_properties(self, app: str, layer: str) -> List[Dict[str, Any]]:
        """Return transformation properties for Bronze/Silver/Gold layers."""
        return self.data.get(app, {}).get(layer, {}).get("Properties", [])

    # ---------- Search / Utility ----------
    def search_by_source_table(self, table: str) -> List[Dict[str, str]]:
        """Find where a given source table is used (across all apps/layers)."""
        results = []
        for app, layers in self.data.items():
            for layer, details in layers.items():
                if layer == "Staging":
                    # Staging DB sources
                    for db in details.get("Source", {}).get("Database", []):
                        if db.get("Source_table") == table:
                            results.append({"App": app, "Layer": layer, "Target": db.get("Target_Iceberg_Table")})
                else:
                    # Bronze/Silver/Gold Properties
                    for prop in details.get("Properties", []):
                        if prop.get("Source_Table") == table:
                            results.append({"App": app, "Layer": layer, "Target": prop.get("Target_Table")})
        return results

    # ---------- Lineage Tracing ----------
    def trace_lineage(self, app: str, start_table: str) -> List[str]:
        """
        Trace table lineage across layers (Staging → Bronze → Silver → Gold).
        """
        lineage = [start_table]
        current = start_table

        for layer in ["Bronze", "Silver", "Gold"]:
            props = self.get_properties(app, layer)
            for p in props:
                if p.get("Source_Table") == current:
                    target = p.get("Target_Table")
                    lineage.append(target)
                    current = target
                    break  # move to next layer

        return lineage