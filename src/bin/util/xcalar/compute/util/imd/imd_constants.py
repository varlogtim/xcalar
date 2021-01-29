from enum import Enum


class IMDLock(Enum):
    LOCKED = 0
    UNLOCKED = 1


class IMDApi(Enum):
    # the operations which make transactions of group
    NEW = 0
    MERGE = 1
    RESTORE = 2
    ALTER = 3


class IMDGroupState(Enum):
    # the current state of the group
    NEW = 0
    IN_USE = 1
    DELETE = 2


class IMDTxnState(Enum):
    BEGIN = 0
    COMMITTED = 1
    FAILED = 2


class IMDOpCode(Enum):
    DELETE = 0
    INSERT = 1
    MODIFY = 2
    UPSERT = 3
    DELETEIFEXIST = 4


class SqlDataType(Enum):
    INTEGER = 0
    BOOLEAN = 1
    FLOAT = 2
    STRING = 3
    TIMESTAMP = 4
    MONEY = 5

    @staticmethod
    def types():
        return set(map(lambda t: t.name, SqlDataType))


# IMD meta store constants
Group_id = 'group_id'
Txn_id = 'txn_id'
Table_id = 'table_id'
Txn_state = 'state'
Group_state = 'state'
Details = 'details'
Start_ts = 'start_timestamp'
End_ts = 'end_timestamp'
TS = 'timestamp'
Action = 'action'
Table_ids = 'table_ids'
Latest_schema_version = 'latest_schema_version'
Last_committed_txn_counter = 'last_committed_txn_counter'
Current_txn_counter = 'current_txn_counter'
Restore_from_txn_counter = 'restore_txn_counter'
Group_counter = 'group_counter'
Failure_reason = 'failure_reason'

Back_xcalar_table = 'backing_xcalar_table'
PK = 'primary_keys'
Schema = 'schema'
Table_name = 'table_name'
Group_name = "group_name"
Col_name = 'name'
Col_type = 'type'

# snapshot constants
Incremental = 'incremental'
Increment_from = 'increment_from'
Snapshot_checksum = 'snapshot_checksum'

# delta store locations
Delta_txn_path = "{backing_store_path}/delta/{txn_id}"
Delta_data_path = Delta_txn_path + "/{table_id}"
Snapshot_txn_path = "{backing_store_path}/snapshot/{txn_id}"
Snapshot_data_path = Snapshot_txn_path + "/{table_id}"

# prefixes for keys for easy lookups
Group_prefix = "IMD_group_{group_id}"
Table_prefix = "IMD_table_{table_id}"

# locks non-persist keys
Imd_non_persist_prefix = "IMD_ONLY_NP_PREFIX_"
Txn_lock_key = Imd_non_persist_prefix + Group_prefix + "_txn_lock"
Snapshot_lock_key = Imd_non_persist_prefix + Group_prefix + "_snapshot_lock"
Group_active_key = Imd_non_persist_prefix + Group_prefix + "_active"
Group_creation_key = Imd_non_persist_prefix + "IMD_Group_Creation_Key"
Group_delete_table_key = Imd_non_persist_prefix + Group_prefix + "_deleting_table"
Imd_table_ref_key = Imd_non_persist_prefix + "{table_name}"

# group keys
Group_name_id_map = "IMD_group_id_mapping_{group_name}"
Group_info_key = Group_prefix + "_info"
Group_detail_key = Group_prefix + "_detail_txns_{group_counter}"

# table keys
Table_name_id_map = "IMD_table_name_id_mapping_{table_name}"
Table_info_key = Table_prefix + "_info"
Table_schema_key = Table_prefix + "_schema_{schema_version}"
Table_delta_key = Table_prefix + "_delta_txn_{group_counter}"
Df_delta_key = Table_prefix + "_delta_txn_{group_counter}_df"

# txns/snapshots
Snapshot_info_key = Group_prefix + "_snapshot_info"
Snapshot_detail_key = Group_prefix + "_snapshot_detail_{group_counter}"

Table_col_types_map = {
    "DfString": "STRING",
    "DfInt32": "INTEGER",
    "DfUInt32": "INTEGER",
    "DfInt64": "INTEGER",
    "DfUInt64": "INTEGER",
    "DfFloat32": "FLOAT",
    "DfFloat64": "FLOAT",
    "DfBoolean": "BOOLEAN",
    "DfTimespec": "TIMESTAMP",
    "DfMoney": "MONEY",

    # reverse map
    "STRING": "DfString",
    "INTEGER": "DfInt64",
    "BOOLEAN": "DfBoolean",
    "FLOAT": "DfFloat64",
    "TIMESTAMP": "DfTimespec",
    "MONEY": "DfMoney"
}
Target_name = 'target_name'
Backing_store_path = 'backing_store_path'
Opcode = "XcalarOpCode"
Rankover = "XcalarRankOver"
Export_driver = "snapshot_csv"

Default_target = "TableStore"
Table_generator_target = "TableGen"

# Retry for increase/decrease table ref count
Table_ref_retry = 10
