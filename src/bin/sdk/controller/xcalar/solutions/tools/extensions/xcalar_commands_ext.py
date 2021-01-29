from colorama import Style


def dispatcher(line):

    usage = f"""
Xcalar Shell Commands:
    {Style.BRIGHT}xcalar_cluster{Style.RESET_ALL}           - lists xcalar cluster configs.
                    - lists cgroup configs on each cluster node

    {Style.BRIGHT}controller_app{Style.RESET_ALL}  - starts the controller app session, reads the universe in the controller app.
                    - gets the current state of the controller app
                    - cancels the controller app
                    - materializes base data into session in the controller app
                    - materializes starting from another univere's snapshot.
                    - continues materializing the base data into a dirty session in the controller app
                    - starts running incremental in the controller app
                    - starts running incremental for a set number of times in the controller app
                    - adjusts the universe with the criteria
                    - adjusts the offset table with the criteria
                    - pauses the incremental in the controller app
                    - manually snapshots the universe
                    - lists all the snapshots.
                    - recovers specific snapshot
                    - recovers latest snapshot
                    - reset the controller state

    {Style.BRIGHT}sdlc{Style.RESET_ALL}            - checkin optimized dataflows with dependent interactive dataflows to git
                    - checkout interactive dataflows from git

    {Style.BRIGHT}sql{Style.RESET_ALL}             - executes sql and displays the result

    {Style.BRIGHT}stats_mart{Style.RESET_ALL}      - upload stats

    {Style.BRIGHT}table{Style.RESET_ALL}           - lists all tables in the session.
                    - lists all information for tables in the session.
                    - shows count of rows in the table.
                    - deletes the table along with metadata.
                    - pins tables.
                    - unpin tables.

    {Style.BRIGHT}xdbridge{Style.RESET_ALL}        - creates linkins pointing to all universe base/delta tables.
                    - creates linkins pointing to all latest session tables.
                    - creates linkins pointing to all tables required by a refiner.
                    - creates parameterized linkins pointing to all universe base/delta tables.
                    - creates parameterized linkins pointing to all latest session tables.
                    - creates parameterized linkins pointing to all tables required by a refiner.

    {Style.BRIGHT}monitor{Style.RESET_ALL}         - monitor command allow user to execute command on individual vms
                    - monitor has a set of tools for system-stat, cgroup, xcalar-service
                    - [system-stat] such as, htop, iotop, lsof,mpstat, pidstat, ss, top, vmstat
                    - [cgroup] such as, systemd-cgls, systemd-cgtop, systemd-run
                    - [xcalar-service] such as, xcalar-usrnode.service, xccli

    {Style.BRIGHT}tmux{Style.RESET_ALL}            - terminal multiplexer
    """
    '''

    {Style.BRIGHT}controller{Style.RESET_ALL}      - materializes base data into session.
                    - continues materializing base data into session.
                    - starts running incremental (num times).
                    - starts the controller session, reads the universe.

    {Style.BRIGHT}dataset{Style.RESET_ALL}         - lists all datasets.
                    - shows content in the dataset.
                    - shows count of rows in the dataset.
                    - shows y rows starting at x in the dataset.
                    - deletes the dataset.

    {Style.BRIGHT}workbook{Style.RESET_ALL}        - lists all worbooks.
                    - lists all information for workbooks.

    {Style.BRIGHT}universe{Style.RESET_ALL}        - shows info of a universe table, including schema, schema upper case, primary keys, etc.
                    - drops a table along with metadata.
                    - adds a table.
                    - replaces a table.
                    - adjusts offset for a topic on one or several partitions.

    '''
    print(usage)


def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(dispatcher, 'line', magic_name='xcalar')


def unload_ipython_extension(ipython):
    pass
