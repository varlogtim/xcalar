import json
import glob
import os


def main(inBlob):
    args = json.loads(inBlob)
    action = args.get('action', None)
    session = args['session']
    if action == 'clenaup':
        pattern = args.get('pattern', '')
        for f in glob.glob(f"/tmp/{session}_{pattern}*"):
            os.remove(f)
    elif action == 'share':
        with open(f"/tmp/{session}_{args['filepath']}", 'w') as fp:
            fp.write(str(args['properties']))
    else:
        raise Exception(f'PropShareApp: Invalid Action {action}')
