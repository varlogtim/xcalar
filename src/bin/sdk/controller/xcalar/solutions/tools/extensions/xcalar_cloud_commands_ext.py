from colorama import Style


def dispatcher(line):

    usage = f"""
Xcalar Shell Commands:

    {Style.BRIGHT}xpip{Style.RESET_ALL}             - python packages management tool.

    {Style.BRIGHT}logmart{Style.RESET_ALL}          - upload logs

    {Style.BRIGHT}stats_mart{Style.RESET_ALL}       - upload stats

    {Style.BRIGHT}systemctl{Style.RESET_ALL}        - manage services (start/ stop/ reload ...etc.)

    {Style.BRIGHT}config_editor{Style.RESET_ALL}    - edit xcalar cluster configuration.

    {Style.BRIGHT}systemstat{Style.RESET_ALL}       - system stat tools for cluster nodes. (htop, iostat, perf ...etc.)

    {Style.BRIGHT}xcalar_cluster{Style.RESET_ALL}   - list xcalar cluster configuration.

    """
    print(usage)


def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(dispatcher, 'line', magic_name='xcalar')


def unload_ipython_extension(ipython):
    pass
