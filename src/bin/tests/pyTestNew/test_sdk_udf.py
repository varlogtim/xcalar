import re

from xcalar.external.client import Client

# This test runs through the sanity flows for the new UDF SDK APIs for both
# workbook and shared UDFs, including exercising any private methods (which are
# prefixed with a underscore - e.g. udf._get())


#
# Parts of this function will be visible in user-visible documentation (the
# parts bracketed by [START ...] and [END...] lines
#
def test_sdk_udf_sanity():
    # [START create_udf_module_in_workbook]
    # from xcalar.external.client import Client
    # from xcalar.external.workbook import Workbook

    # create a new UDF module in a workbook

    # The udf function for workbook
    udf_wb_name = 'udf_demo_module'
    udf_wb_src = """def demo_foo(arg):
                     return 'foo'"""
    client = Client()
    workbook = client.create_workbook("my_workbook")
    udf = workbook.create_udf_module(udf_wb_name, udf_wb_src)
    # [END create_udf_module_in_workbook]

    get_demo_source = udf._get()
    assert (udf_wb_src == get_demo_source)

    # [START create_shared_udf_module]
    # from xcalar.external.client import Client

    # create a new shared UDF module
    #
    # The shared udf function
    udf_shared_name = 'udf_demo_shared_module'
    udf_shared_src = """def demo_shared_foo(arg):
                     return 'foo'"""

    sudf = client.create_udf_module(udf_shared_name, udf_shared_src)
    # [END create_shared_udf_module]

    shared_udf_contents = sudf._get()

    assert (shared_udf_contents == udf_shared_src)

    # [START delete_udf_module]
    # from xcalar.external.udf import UDF

    # delete the UDF module
    # NOTE: udf could have been returned from workbook.create_udf_module() or
    # from client.create_udf_module() - i.e. it could be a workbook UDF or a
    # shared UDF
    udf.delete()

    # [END delete_udf_module]

    udf = workbook.create_udf_module(udf_wb_name, udf_wb_src)
    assert (udf_wb_src == udf._get())

    # [START update_udf_module]
    # from xcalar.external.udf import UDF

    # update the UDF module (also shows how to get a UDF's source code)
    udf_wb_src2 = "def bar(arg):\n return 'bar'\ndef barbar(arg1, arg2):\n return 'barbar'"

    # NOTE: udf could be a workbook UDF or a shared UDF
    udf.update(udf_wb_src2)
    # [END update_udf_module]

    # Test _get() and _get_path() UDF methods for workbook UDFs
    udf = workbook.get_udf_module(udf_wb_name)
    assert (udf._get() == udf_wb_src2)
    path_should_be = "/workbook/{}/{}/udf/{}".format(
        workbook.username, workbook._Workbook__id, udf_wb_name)
    assert (udf._get_path() == path_should_be)

    # Test listing of funcs in a UDF module
    funcs = []
    funcsListOut = udf._list("*")
    if funcsListOut.numXdfs >= 1:
        for uix in range(0, funcsListOut.numXdfs):
            fq_fname = funcsListOut.fnDescs[uix].fnName
            fq_mname, fname = re.match(r'(.*):(.*)', fq_fname).groups()
            funcs.append(fname)
    checkfuncs = ['bar', 'barbar']
    for fix in range(0, len(funcs)):
        print("func name is {}".format(funcs[fix]))

    # Use set() to diff the two func lists to verify that udf._list() works
    # set() effectively dedupes the list
    assert (list(set(funcs) - set(checkfuncs)) == [])

    udf_wb_name2 = "udf_demo_module2"
    udf = workbook.create_udf_module(udf_wb_name2, udf_wb_src)
    assert (udf_wb_src == udf._get())

    # Test list_udf_modules for workbook
    mods_should_be = [udf_wb_name, udf_wb_name2]
    mod_objs_are = workbook.list_udf_modules()
    mods_are = []
    for mo in mod_objs_are:
        mods_are.append(mo.name)
    print("mod list is {}".format(mods_are))
    for mix in range(0, len(mods_are)):
        print("udf mod name is {}".format(mods_are[mix]))

    assert (list(set(mods_are) - set(mods_should_be)) == [])

    # Validate that get_udf_module() fails for non-existent module
    try:
        udf = workbook.get_udf_module("non-existing")
    except ValueError as e:
        assert (e.args[0] == "No such UDF module: 'non-existing'")

    workbook.inactivate()
    workbook.delete()

    #
    # Rest of tests below are about shared UDFs sanity
    #
    # sudf was already created above; now update it
    udf_shared_src2 = "def bar(arg):\n return 'bar'\ndef barbar(arg1, arg2):\n return 'barbar'"

    sudf.update(udf_shared_src2)
    assert (sudf._get() == udf_shared_src2)

    # Test _get() and _get_path() UDF methods for shared UDFs
    sudf = client.get_udf_module(udf_shared_name)
    assert (sudf._get() == udf_shared_src2)
    shared_mod_path_should_be = "/sharedUDFs/{}".format(udf_shared_name)
    assert (sudf._get_path() == shared_mod_path_should_be)

    # Test ability to list funcs in a shared UDF module
    sfuncs = []
    sfuncsListOut = sudf._list("*")
    if sfuncsListOut.numXdfs >= 1:
        for uix in range(0, sfuncsListOut.numXdfs):
            fq_sfname = sfuncsListOut.fnDescs[uix].fnName
            fq_smname, sfname = re.match(r'(.*):(.*)', fq_sfname).groups()
            sfuncs.append(sfname)

    checksfuncs = ['bar', 'barbar']
    for fix in range(0, len(sfuncs)):
        print("func name is {}".format(sfuncs[fix]))

    assert (list(set(sfuncs) - set(checksfuncs)) == [])

    # Ensure that get_udf_module() for a shared UDF module fails as expected
    try:
        sudf2 = client.get_udf_module("non-existing")
    except ValueError as e:
        assert (e.args[0] == "No such UDF module: 'non-existing'")

    udf_shared_name2 = "udf_demo_shared_module2"
    sudf2 = client.create_udf_module(udf_shared_name2, udf_shared_src)
    assert (udf_shared_src == sudf2._get())

    # Ensure that list_udf_modules for shared space works as expected
    mods_should_be = [udf_shared_name, udf_shared_name2]
    mods_are = []
    mod_objs_are = []
    # Use "udf_*" pattern instead of the default "*" - otherwise list will
    # return the 'default' module name too
    mod_objs_are = client.list_udf_modules("udf_*")
    for mo in mod_objs_are:
        mods_are.append(mo.name)
    print("modlist is {}".format(mods_are))

    # set() effectively dedupes the list
    assert (list(set(mods_are) - set(mods_should_be)) == [])

    sudf2.delete()
    sudf.delete()
