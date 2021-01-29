import requests
from requests.auth import HTTPBasicAuth
import json
import xcalar.container.context as ctx
import logging
logger = logging.getLogger('xcalar')

MAXRESULTS = 50
url = 'https://xcalar.atlassian.net/rest/api/3/'
jql = 'labels%20%3D%20XCQA'
auth = HTTPBasicAuth('nogievetsky@xcalar.com', 'ynSWqhscHPcp8S4ttErJE28F')
headers = {'Accept': 'application/json'}


def get_content(content, text=''):
    text = []
    for i in content:
        if i['type'] == 'text':
            text.append(i['text'])
        elif i['type'] == 'paragraph':
            text += get_content(i['content'])
        else:
            continue
    return text


# def loadJQL(x, y):
def parse(x, y):
    xpu = ctx.get_xpu_id()
    # if xpu != 0: return
    # xpu_r = xpu % (ctx.get_xpu_cluster_size() - 1)
    startAt = xpu * MAXRESULTS + 1
    logger.info(
        f'{url}search?startAt={startAt}&maxResults={MAXRESULTS}&jql={jql}')
    # yield {'xpu':xpu,'startAt':startAt, 'total':-1, 'maxResults':MAXRESULTS}

    response = requests.request(
        'GET',
        f'{url}search?startAt={startAt}&maxResults={MAXRESULTS}&jql={jql}',
        headers=headers,
        auth=auth)
    logger.info(response.text)
    jrs = json.loads(response.text)
    if int(jrs['startAt']) != startAt:
        return
    yield {
        'xpu': xpu,
        'startAt': jrs['startAt'],
        'total': jrs['total'],
        'maxResults': jrs['maxResults']
    }
    issues = jrs.get('issues', [])
    for issue in issues:
        yield {'xpu': xpu, **get_dict(issue)}
    # if 'issues' in jrs:
    #     for it in jrs['issues']:
    #         yield {'xpu':ctx.get_xpu_id(), **get_dict(it)}


def get_dict(issue):
    #     print(issue['fields'].keys())
    outd = {'key': issue['key']}
    for f, item in issue['fields'].items():
        if type(item) == dict:
            if 'name' in item:
                outd[f] = item['name']
            elif 'displayName' in item:
                outd[f] = item['displayName']
            elif 'content' in item:
                outd[f] = ' '.join(get_content(item['content']))
            else:
                continue
                print('>>>', f, item)
        elif type(item) == list:
            if f == 'issuelinks':
                try:
                    item = [f"{x['type']['outward']}:{x['outwardIssue']['key']}" for x in item if 'outwardIssue' in x] +\
                           [f"{x['type']['inward']}:{x['inwardIssue']['key']}" for x in item if 'inwardIssue' in x]
                except Exception as e:
                    print('RRR>>', item)
            elif f == 'customfield_10028':
                f = 'customers'
            elif f == 'customfield_10021':
                f = 'sprint'
            elif f == 'components':
                item = [x['name'] for x in item]
            elif f == 'labels':
                pass
            elif f in ['fixVersions']:
                item = [i['name'] for i in item]
            else:
                continue
            outd[f] = '|'.join(item)


#                 print('>>>',f, item)
#                 try:
#                     outd[f] = '|'.join(item)
#                 except Exception as e:
#                     print('RRR>>',item)
        elif type(item) == str:
            if f in [
                    'created', 'updated', 'summary', 'customfield_10014',
                    'lastViewed'
            ]:
                outd[f] = item
            else:
                continue
    #             print('^^^',f,type(item), item)
        else:
            continue
    #         print('???',f,type(item), item)

    return outd
