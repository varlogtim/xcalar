from xcalar.external.table import Table

SNAPSHOT_KEY = 'snapshot_{}_fb230b2b-d497-4606-be1f-918d23b4ff73'
SNAPSHOT_FOLDER_FORMAT = '{}/{}'
SNAPSHOT_TABLE_FORMAT = '{}/{}/{}'


class SnapshotManagement():
    def __init__(self, table: Table, folder: str):
        self.table = table
        self.folder = folder

        # schema = OrderedDict()
        # for col in table.get_meta()['valueAttrs']:
        #     schema[col['name']] = DfFieldTypeT()._VALUES_TO_NAMES[col['type']]
        # return schema
