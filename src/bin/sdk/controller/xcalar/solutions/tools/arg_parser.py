from colorama import Style
import re
global ext_usage


class ArgParser:
    def __init__(self, ext_name, usage=[]):
        self.ext_name = ext_name
        self.usage = usage

    def get_usage(self):
        usage = 'Usage:\n'
        for u in self.usage:
            verbs = ' '.join([f'{k} <{v}>' for k, v in u['verbs'].items()])
            usage += f'{Style.DIM}> {Style.NORMAL}{self.ext_name} {Style.DIM}-{verbs} {Style.RESET_ALL} {Style.BRIGHT}- {u["desc"]}\n{Style.RESET_ALL}'
        return usage

    def get_params(self, line):
        input_params = {}
        for tkn in f' {line.strip()}'.split(' -'):
            if len(tkn):
                # kv = tkn.strip().split(' ')
                kv = [
                    k.strip()
                    for k in re.compile('("[^"]+"|[^\s"]+)').split(tkn)
                    if k.strip() != ''
                ]
                if len(kv) > 2:
                    input_params[kv[0].strip()] = ' '.join(kv[1:])
                elif len(kv):
                    input_params[kv[0].strip()] = kv[-1].replace('"', '')
        return input_params

    def init_params(self, input_params, defaults):
        params = defaults
        for k, v in params.items():
            if k in input_params:
                params[k] = input_params[k]
                print(f'-> {k}, setting to {params[k]}')
            else:
                print(f'-> {k} not provided, setting to {v}')
        return params

    def get_prompt_completers(self):
        verbs = [u['verbs'] for u in self.usage]
        return sum(verbs, [])

    def register_magic_function(self, ipython, dispatcher):
        global ext_usage
        ext_usage = sum([[f'-{k}' for k in u['verbs']] for u in self.usage],
                        [])

        def prompt_completers(self, event):
            return ext_usage

        ipython.register_magic_function(
            dispatcher, 'line', magic_name=self.ext_name)
        ipython.set_hook(
            'complete_command', prompt_completers, str_key=self.ext_name)
        ipython.set_hook(
            'complete_command', prompt_completers, str_key=f'%{self.ext_name}')
