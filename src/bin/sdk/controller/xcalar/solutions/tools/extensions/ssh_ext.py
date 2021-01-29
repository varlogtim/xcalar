def dispatcher(line):

    usage = """
Usage:
    ssh - command not defined.

    """

    tokens = line.split()

    try:

        # session destroy
        if len(tokens) == 0:
            print(f"command is not defined.")

        else:
            print(usage)

    except Exception as e:
        print(e)


def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(dispatcher, "line", magic_name="ssh")
    ipython.register_magic_function(dispatcher, "line", magic_name="ssh2")


def unload_ipython_extension(ipython):
    pass
