import csv

from xcalar.external.client import Client

# This file creates the data target types table, which is shown in the documentation
if __name__ == "__main__":
    client = Client()

    types_info = client._list_data_target_types()

    csv_file = open("list_of_data_target_types.csv", "w")
    csv_writer = csv.writer(csv_file)

    csv_writer.writerow(["type_id", "type_name", "description", "parameters"])

    for type_info in types_info:
        params = type_info["parameters"]
        if params == []:
            param_str = ""
        else:
            param_str = ""
            for param in params:
                name = param["name"]
                description = param["description"]
                param_str += ("* **" + name + "**" + " : *")
                if (param["optional"]):
                    param_str += "Optional"
                else:
                    param_str += "Required"
                param_str += ".* "
                param_str += (description + "\n")
            param_str += "\n "

        description = type_info["description"].replace(";http", "; http")
        description = description.replace("... ", "\n")
        csv_writer.writerow([
            type_info["type_id"], type_info["type_name"], description,
            param_str
        ])

    csv_file.close()
