import json
import os
from pathlib import Path
from xcalar.solutions.universe_adapter import UniverseAdapter


class XcalarUniverseAdapter(UniverseAdapter):
    def getUniverse(self, key):
        if key.startswith('ref_rig_'):
            template_file = os.path.dirname(
                Path(__file__)) + '/test_rig_refdata_universe.json'
            with open(template_file, 'r') as f:
                data = json.load(f)
            return data
        template_file = os.path.dirname(
            Path(__file__)) + '/test_rig_universe.json'
        with open(template_file, 'r') as f:
            data = json.load(f)
        return data

    def getSchemas(self, names=[]):
        schema_file = os.path.dirname(Path(__file__)) + '/gs_schema.json'
        with open(schema_file, 'r') as f:
            return json.load(f)
