import subprocess
import shutil
import glob
import os
from colorama import Style

from xcalar.solutions.tools.utilities.xcalar_config import get_xcalar_properties


def dispatcher(line):

    usage = f"""
Usage:
    pip <command> [options]

Commands:
    {Style.NORMAL}xpip {Style.DIM}install{Style.RESET_ALL}                                     {Style.BRIGHT}- Install packages.{Style.RESET_ALL}
    {Style.NORMAL}xpip {Style.DIM}uninstall{Style.RESET_ALL}                                   {Style.BRIGHT}- Uninstall packages.{Style.RESET_ALL}
    {Style.NORMAL}xpip {Style.DIM}freeze{Style.RESET_ALL}                                      {Style.BRIGHT}- Output installed packages in requirements format.{Style.RESET_ALL}
    {Style.NORMAL}xpip {Style.DIM}list{Style.RESET_ALL}                                        {Style.BRIGHT}- List installed packages.{Style.RESET_ALL}

    """

    properties = get_xcalar_properties()
    PYSITE = properties['PYSITE']

    line = line.replace(' -t ', '')
    tokens = line.split()

    try:
        # ---------------------
        # connectivity check
        # ---------------------
        site_packages = PYSITE
        if len(tokens) == 0:
            print(usage)

        elif tokens[0] == 'install':
            if not os.path.exists(site_packages):
                os.makedirs(site_packages)

            packages = ' '.join(tokens[1:])
            cmd = f'/opt/xcalar/bin/python3 -m pip install -t {site_packages} {packages}'
            returned_output = subprocess.call(cmd, shell=True)
            print(returned_output)

        elif len(tokens) == 3 and tokens[0] == 'install' and tokens[1] == '-r':
            if not os.path.exists(site_packages):
                os.makedirs(site_packages)

            requirements_txt = tokens[2]
            cmd = f'/opt/xcalar/bin/python3 -m pip install -t {site_packages} -r {requirements_txt}'
            returned_output = subprocess.call(cmd, shell=True)
            print(returned_output)

        elif tokens[0] == 'uninstall':
            # go to PYSITE
            # remove it
            packages = tokens[1:]
            for package in packages:
                matched_dirs = list()
                matched_dirs.extend(glob.glob(f'{site_packages}/{package}'))
                matched_dirs.extend(glob.glob(f'{site_packages}/{package}-*'))
                matched_dirs.extend(glob.glob(f'{site_packages}/{package}.*'))

                for folder in matched_dirs:
                    if os.path.exists(folder):
                        shutil.rmtree(folder)
            if matched_dirs:
                print(f'{tokens[1:]} deleted.')
            else:
                print(f'{tokens[1:]} not found.')

        elif tokens[0] == 'freeze':
            filename = ''.join(tokens[1::]).replace('>', '')
            print(filename)
            cmd = f'/opt/xcalar/bin/python3 -m pip freeze > {filename}'
            returned_output = subprocess.call(cmd, shell=True)
            print(returned_output)

        elif tokens[0] == 'list':
            packages = glob.glob(f'{site_packages}/*.*-info')
            print(f'Package                  Version         Location')
            print(
                '------------------------ --------------- --------------------------------------------'
            )
            for p in packages:
                p = p.replace(f'{site_packages}/', '')
                name = p.split('-', 1)[0]
                version = p.split('-', 1)[1]
                version = version.replace('.dist-info', '')
                print(f'{name:25s}{version:10s}')

        else:
            print(usage)

    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return [
            'install',
            'uninstall',
            'list',
            'freeze',
        ]

    try:
        ipython.register_magic_function(dispatcher, 'line', magic_name='xpip')
        ipython.set_hook('complete_command', auto_completers, str_key='xpip')
        ipython.set_hook('complete_command', auto_completers, str_key='%xpip')
    except Exception as e:
        print(e)


def unload_ipython_extension(ipython):
    pass
