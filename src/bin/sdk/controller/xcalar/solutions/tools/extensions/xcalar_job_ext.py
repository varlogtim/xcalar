import os
from xcalar.compute.util.xcalar_job import XcalarJob
from colorama import Style


def dispatcher(line):

    help = {
        'show_all':
            f"""
Usage:

XCALAR Job Mangmement

    {Style.NORMAL}xcalar_job {Style.DIM}-c/ -jobConfig <conf> {Style.RESET_ALL}                    {Style.BRIGHT}- job config json.{Style.RESET_ALL}
    {Style.NORMAL}xcalar_job {Style.DIM}-l/ listJobs {Style.RESET_ALL}                             {Style.BRIGHT}- list job.{Style.RESET_ALL}
    {Style.NORMAL}xcalar_job {Style.DIM}-d/ deleteJob <store> {Style.RESET_ALL}                    {Style.BRIGHT}- delete job.{Style.RESET_ALL}
    {Style.NORMAL}xcalar_job {Style.DIM}-v/ viewJob   <store> {Style.RESET_ALL}                    {Style.BRIGHT}- view job.{Style.RESET_ALL}

""",
    }

    tokens = line.split()
    if len(tokens) == 0:
        print(help['show_all'])
        return

    try:
        if os.environ['WORKBUCKET']:
            xcalar_job = XcalarJob(os.environ['WORKBUCKET'])
        else:
            xcalar_job = XcalarJob()

        if tokens[0] in ['-c', '-jobConfig'] and tokens[1]:
            print(xcalar_job.create(tokens[1]))

        elif tokens[0] in ['-l', '-listJobs']:
            for item in xcalar_job.list():
                print(item)

        elif tokens[0] in ['-d', '-deleteJob'] and tokens[1]:
            xcalar_job.delete(tokens[1])
            print(f'{tokens[1]} ... job deleted.')

        elif tokens[0] in ['-v', '-viewJob'] and tokens[1]:
            # if confirm() is False:
            #     return
            result = xcalar_job.view(tokens[1])    # options.viewJob
            print(result)

        else:
            print(help['show_all'])

    except AttributeError as ex:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {ex}')
    except Exception as e:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} {e}')
    except KeyboardInterrupt:
        print(f'{Style.DIM}Warning: {Style.RESET_ALL} Ctrl-c detected.')
        return


def load_ipython_extension(ipython, *args):
    def auto_completers(self, event):
        return [
            '-c', '-jobConfig', '-l', '-listJobs', '-d', '-deleteJob', '-v',
            '-viewJob'
        ]

    ipython.register_magic_function(
        dispatcher, 'line', magic_name='xcalar_job')
    ipython.set_hook('complete_command', auto_completers, str_key='xcalar_job')
    ipython.set_hook(
        'complete_command', auto_completers, str_key='%xcalar_job')


def unload_ipython_extension(ipython):
    pass
