import csv

from xcalar.external.xcalarapi import Client


# creates a csv file with a list of metrics (all info apart from value ofc)
def metrics_update(client):
    csv_file = open("list_of_metrics.csv", "w")
    csv_writer = csv.writer(csv_file)

    csv_writer.writerow(
        ["index", "metric_name", "metric_type", "group_id", "group_name"])
    node_metrics = client.get_metrics_from_node(0)
    index = 0
    for m in node_metrics:
        csv_writer.writerow(
            [index, m.metric_name, m.metric_type, m.group_id, m.group_name])
        index += 1
    csv_file.close()


# creates a csv file with a list of config_params (all info apart from value ofc)
def config_params_update(client):
    csv_file = open("list_of_params.csv", "w")
    csv_writer = csv.writer(csv_file)

    csv_writer.writerow([
        "index", "param_name", "visible", "changeable", "restart_required",
        "default_value"
    ])
    params = client.get_config_params()
    index = 0
    for p in params:
        csv_writer.writerow([
            index, p.param_name, p.visible, p.changeable, p.restart_required,
            p.default_value
        ])
        index += 1
    csv_file.close()


if __name__ == "__main__":
    client = Client()
    metrics_update(client)
    config_params_update(client)
