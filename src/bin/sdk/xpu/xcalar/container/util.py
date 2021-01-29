import re


# this method cleanse a string to a valid xcalar column
def cleanse_column_name(column_name, capitalize=False):
    col_name = column_name.strip()
    col_name = re.sub(r'^[^a-zA-Z_]', "_", col_name)
    col_name = col_name[0:256]
    if col_name in ["DATA", "None", "True", "False"]:
        col_name = "_" + col_name
    col_name = re.sub(r"[^a-zA-Z0-9_]", "_", col_name)
    return col_name.upper() if capitalize else col_name
