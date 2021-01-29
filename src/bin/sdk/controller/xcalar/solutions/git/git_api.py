import json
import requests
import urllib


class GitApi:
    def __init__(self, git_url, git_project_id, access_token):
        self.git_url = git_url
        self.git_project_id = git_project_id
        self.access_token = access_token

    def listTree(self, branch, path):
        r = requests.get(
            '{}/projects/{}/repository/tree?ref={}&path={}&per_page=100'.
            format(self.git_url, self.git_project_id, branch, path),
            headers={'Private-Token': self.access_token})
        r.raise_for_status()
        return r.json()

    def special_encode(self, path):
        separator = urllib.parse.quote_plus('/')
        tokens = path.split('/')
        encoded = tokens[0]
        for i in range(1, len(tokens)):
            encoded = encoded + separator + urllib.parse.quote(tokens[i])
        return encoded

    def get(self, branch, path):
        url = '{}/projects/{}/repository/files/{}/raw?ref={}'.format(
            self.git_url, self.git_project_id, self.special_encode(path),
            branch)
        r = requests.get(url, headers={'Private-Token': self.access_token})
        r.raise_for_status()
        return r.text

    def exists(self, path, branch):
        url = '{}/projects/{}/repository/files/{}?ref={}'.format(
            self.git_url, self.git_project_id, self.special_encode(path),
            branch)
        # logging.error(url)
        r = requests.get(url, headers={'Private-Token': self.access_token})

        # logging.error(str(r.status_code))
        if r.status_code == 404:
            return False

        r.raise_for_status()
        return True

    # See https://docs.gitlab.com/ee/api/commits.html#create-a-commit-with-multiple-files-and-actions
    # Content is a dict of {path, data}
    def commit_files(self, branch, commit_message, content={}):
        actions = []
        for path, data in content.items():
            if self.exists(path, branch):
                action = 'update'
            else:
                action = 'create'

            actions.append({
                'action': action,
                'file_path': path,
                'content': data
            })
        payload = {
            'branch': branch,
            'commit_message': commit_message,
            'actions': actions
        }

        r = requests.post(
            '{}/projects/{}/repository/commits'.format(self.git_url,
                                                       self.git_project_id),
            headers={'Private-Token': self.access_token},
            json=payload)
        r.raise_for_status()
        return r.json()

    def pretty_print_query(self, query_string):
        return json.dumps(json.loads(query_string), sort_keys=True, indent=4)

    def pretty_print_kvsvalue(self, kvs_json):
        return json.dumps(kvs_json, sort_keys=True, indent=4)

    # add dict parameter to root dataflows object containing the other root dataflows
    # source_dataflows {
    #   name : kvstorecontent (another dict)
    # }
    def get_content_for_dataflows(self, dataflows, path):
        content = {}
        for df in dataflows['target_dataflows']:
            query = self.pretty_print_query(df['query_string'])
            content['{}/{}/query_string.json'.format(
                path, df['dataflow_name'])] = query
            optimized_query = self.pretty_print_query(
                df['optimized_query_string'])
            content['{}/{}/optimized_query_string.json'.format(
                path, df['dataflow_name'])] = optimized_query
        for df in dataflows['interactive_dataflows']:
            kvscontent = self.pretty_print_kvsvalue(df['kvs_json'])
            content['{}/{}/kvsvalue.json'.format(
                path, df['dataflow_name'])] = kvscontent
        return content

    def get_content_for_udfs(self, udfs, path):
        content = {}
        for entry in udfs:
            dataflow_name = entry['name']
            for udf in entry['udfs']:
                content['{}/{}/{}.py'.format(path, dataflow_name,
                                             udf.name)] = udf._get()
        return content

    def checkin_dataflows_and_udfs(self,
                                   dataflows,
                                   udfs,
                                   branch,
                                   path,
                                   commit_message='Auto checkin'):
        content = {
            **self.get_content_for_dataflows(dataflows, path),
            **self.get_content_for_udfs(udfs, path)
        }
        self.commit_files(branch, commit_message, content)

    def checkin_dataflows(self,
                          dataflows,
                          branch,
                          path,
                          commit_message='Auto checkin'):
        content = self.get_content_for_dataflows(dataflows, path)
        self.commit_files(branch, commit_message, content)

    def checkin_udfs(self, udfs, branch, path, commit_message='Auto checkin'):
        content = self.get_content_for_udfs(udfs, path)
        self.commit_files(branch, commit_message, content)
