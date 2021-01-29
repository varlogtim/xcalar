import json

from xcalar.container.target.manage import get_target_data, build_target
import xcalar.container.context as ctx
import xcalar.container.cluster

from xcalar.external.client import Client

# Set up logging
logger = ctx.get_logger()

cluster = xcalar.container.cluster.get_running_cluster()


# downloads notebook from xcalar system to given target and path.
def download_notebook_to_target(client, notebook_name, target, path):
    notebook_handle = client.get_workbook(notebook_name)
    logger.debug("Downloading notebook '{}' to '{}' using target '{}'".format(
        notebook_name, path, target.name()))
    with target.open(path, "wb") as fp:
        fp.write(notebook_handle.download())
    return {}


# uploads notebook to xcalar system from given target and path.
def upload_notebook_from_target(client, notebook_name, target, path):
    contents = None
    logger.debug(
        "Uploading notebook '{}' to xcalar from '{}' using target '{}'".format(
            notebook_name, path, target.name()))
    with target.open(path, "rb") as fp:
        contents = fp.read()
    wb = client.upload_workbook(notebook_name, contents)
    return {"session_id": wb._workbook_id}


def main(in_blob):
    if cluster.total_size > 1:
        raise ValueError("Notebook app should run as local app only.")

    logger.info("Received input: {}".format(in_blob))
    ret = None
    try:
        in_obj = json.loads(in_blob)
        target_name = in_obj["connector_name"]
        path = in_obj["notebook_path"]
        notebook_name = in_obj["notebook_name"]

        client = Client(bypass_proxy=True, user_name=in_obj['user_name'])
        target_data = get_target_data(target_name)
        target_obj = build_target(target_name, target_data, path)

        if in_obj["op"] == "upload_from_target":
            ret = upload_notebook_from_target(client, notebook_name,
                                              target_obj, path)
        elif in_obj["op"] == "download_to_target":
            ret = download_notebook_to_target(client, notebook_name,
                                              target_obj, path)
        else:
            raise ValueError("Not implemented")

    except Exception as ex:
        logger.exception("Error upload/download notebook: {}".format(str(ex)))
        raise ex from None

    return json.dumps(ret)
