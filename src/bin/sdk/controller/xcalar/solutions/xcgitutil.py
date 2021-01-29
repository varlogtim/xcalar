"""
A utility to allow git checkin of workbooks
"""
from xcalar.solutions.uber_dispatcher import UberDispatcher
from xcalar.solutions.git.git_api import GitApi
from xcalar.solutions.sdlc import SdlcManager

from xcalar.solutions.xcalar_client_utils import (get_or_create_session,
                                                  get_client, get_xcalarapi)
import argparse
import logging
import logging.config

logging.basicConfig(
    level=logging.INFO,
    format=    # NOQA
    '[%(asctime)s] p%(process)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',    # NOQA
    handlers=[logging.StreamHandler()])


class GitCheckinUtil():
    def checkin(self, args):
        client = get_client(args.xcalar_url, args.xcalar_username,
                            args.xcalar_password)
        xcalarApi = get_xcalarapi(args.xcalar_url, args.xcalar_username,
                                  args.xcalar_password)
        session = get_or_create_session(client, 'xcutil')
        xcalarApi.setSession(session)
        gitApi = GitApi(args.url, args.project_id, args.access_token)
        dispatcher = UberDispatcher(
            client, session=session, xcalarApi=xcalarApi)
        sdlc = SdlcManager(dispatcher, gitApi)

        sdlc.checkin_all_dataflows(
            args.dataflow_names,
            args.branch_name,
            args.path,
            workbook_name=args.workbook_name)


def parseArgs():
    parser = argparse.ArgumentParser(description='Xcalar Workbook Utility')
    parser.add_argument(
        '--url', type=str, required=True, help='Git Repository URL')
    parser.add_argument(
        '--project_id', type=int, required=True, help='Git Project Id')
    parser.add_argument(
        '--access_token', type=str, required=True, help='Git Access Token')
    parser.add_argument(
        '--path',
        type=str,
        required=True,
        help='Path to checkout/checkin dataflow')
    parser.add_argument(
        '--branch_name',
        type=str,
        required=True,
        default='master',
        help='Git Branch Name')
    parser.add_argument(
        '--xcalar_url',
        type=str,
        required=True,
        help='URL of Xcalar cluster to connect to ')
    parser.add_argument(
        '--xcalar_username',
        type=str,
        required=True,
        help='URL of Xcalar cluster to connect to ')
    parser.add_argument(
        '--xcalar_password',
        type=str,
        required=True,
        help='URL of Xcalar cluster to connect to ')
    parser.add_argument(
        '--workbook_name',
        type=str,
        required=True,
        help='Workbook name to download dataflow from')
    parser.add_argument(
        '--dataflow_names',
        type=str,
        required=True,
        nargs='*',
        help='Dataflow names to download and checked in')

    return parser.parse_args()


if __name__ == '__main__':
    args = parseArgs()
    util = GitCheckinUtil()
    util.checkin(args)
    print('Done')
