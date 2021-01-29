import xcalar.container.driver.base as driver


# This driver doesn't do anything. This is useful because retinas must have
# an export driver, but we don't always want to export the data
@driver.register_export_driver(
    name="do_nothing", is_builtin=True, is_hidden=True)
def driver(table):
    """Do nothing for the export. This is useful for when you know you want an
    export node but you only want a placeholder for now."""
    return
