import pytest
import math
import pandas as pd
import time
import decimal
import random
from decimal import Decimal, ROUND_HALF_UP

from xcalar.compute.util.cluster import DevCluster

from xcalar.external.dataflow import Dataflow

pytestmark = [pytest.mark.usefixtures("config_backup")]

num_dataset_rows = 1000

mem_udf_name = "testmoney"
seed = int(time.time())
mem_udf_source = """
import json
import random
import math

def get_integer_part(scale):
    bin_val = random.random() <= 0.5
    if bin_val:
        #  0 to 10^15 (quadrillion, a quantity that might actually be used in finance)
        min_num = 0
        max_num = math.pow(10, 15)
    else:
        #  test the upper bounds of the type
        min_num = math.pow(10, 29-scale)
        max_num = math.pow(10, 33-scale)
    return random.randint(min_num, max_num)

def gen_money_vals(fullPath, inStream, scale, seed=2):
    inObj = json.loads(inStream.read())
    numLocalRows = inObj["numRows"]
    startNum = inObj["startRow"]
    max_scale = int('9'*scale)
    random.seed(seed)

    for x in range(startNum, startNum+numLocalRows):
        isnull = random.random() <= 0.1 # 10% nulls
        if isnull:
            yield {"col1": x, "col2":None}
        else:
            pos = '-' if random.random() <= 0.3 else '' # 30% negatives
            integer_part = '{}'.format(get_integer_part(scale))
            fraction_part = '{}'.format(random.randint(0, max_scale)).zfill(scale)
            yield {"col1": x, "col2":"{}{}.{}".format(pos, integer_part, fraction_part)}
"""


@pytest.fixture(scope="module")
def reset_params(client):
    old_params = client.get_config_params()
    yield
    for param in old_params:
        if param['param_name'] in ["MoneyScaleDigits", "MoneyRescale"]:
            client.set_config_param(param['param_name'], param['param_value'])


@pytest.fixture(scope="module", params=[2, 6])
def money_scale(client, reset_params, request):
    client.set_config_param("MoneyScaleDigits", str(request.param))
    client.set_config_param("MoneyRescale", "true")
    yield request.param


@pytest.fixture(scope="module")
def workbook(client):
    workbook = client.create_workbook("TestMoney")
    yield workbook
    workbook.delete()


@pytest.fixture(scope="module")
def session(workbook):
    session = workbook.activate()
    yield session
    session.destroy()


@pytest.fixture(scope="module")
def gen_dataset(client, workbook, money_scale):
    dataset_name = "testMoneyDs"
    path = str(num_dataset_rows)
    parser_name = "{}:gen_money_vals".format(mem_udf_name)
    parser_args = {"scale": money_scale, "seed": seed}
    target_name = "TableGen"

    # Upload Udf
    workbook._create_or_update_udf_module(mem_udf_name, mem_udf_source)

    dataset_builder = workbook.build_dataset(
        dataset_name,
        target_name,
        path,
        "udf",
        parser_name=parser_name,
        parser_args=parser_args)
    dataset = dataset_builder.load()
    yield dataset
    dataset.delete()


def test_money_basic(client, session, gen_dataset, money_scale):
    print("seed used: {}".format(seed))
    con = decimal.getcontext()
    con.prec = 34
    con.Emax = 6144
    con.Emin = -6143
    con.clear_traps()
    con.rounding = ROUND_HALF_UP

    project_columns = [
        {
            "name": "col1",
            "type": "integer"
        },
        {
            "name": "col2",
            "type": "money"
        },
    ]
    # taking random value to be small to not overflow the results
    # as overflow behaviour is different for xcalar and pandas
    random.seed(seed)
    frac_part = str(random.randint(1, 9)) * money_scale
    rand_decimal = Decimal('{}.{}'.format(random.randint(1, 9), frac_part))

    df = Dataflow.create_dataflow_from_dataset(client, gen_dataset)
    # this is the workaround to set type as DfMoney instead of DfNull
    # this was propely fixed in trunk branch with change in
    # create_dataflow_from_dataset api
    for op in df._query_list:
        if op['operation'] == 'XcalarApiSynthesize':
            for col in op['args']['columns']:
                if col['sourceColumn'] == 'col2':
                    col['columnType'] = 'DfMoney'
    evals_list = [
        ("addNumeric('{}', col2)".format(rand_decimal), "addnumeric_col"),
        ("money(money(col2))", "multinumeric_col"),
        ("ifNumeric(gt(money('{0}'), col2), money('{0}'), money(1))".format(
            rand_decimal), "ifnumeric_col"),
        ("eqNonNull(money('{}'), col2)".format(rand_decimal), "eqnn_col"),
        ("neq(money('{}'), col2)".format(rand_decimal), "neq_col"),
        ("exists(col2)", "exists_col"), ("isNull(col2)", "isnull_col"),
        ("subNumeric(col2, '{}')".format(rand_decimal), "sub_col"),
        ("divNumeric(col2, money('{}'))".format(rand_decimal), "div_col"),
        ("isNumeric(col2)", "isnumeric_col"),
        ("isNumeric(col1)", "isnumeric_int_col"),
        ("eq(money('{}'), col2)".format(rand_decimal), "eq_col"),
        ("gt(money('{}'), col2)".format(rand_decimal), "gt_col"),
        ("ge(money('{}'), col2)".format(rand_decimal), "ge_col"),
        ("lt(money('{}'), col2)".format(rand_decimal), "lt_col"),
        ("le(money('{}'), col2)".format(rand_decimal), "le_col"),
        ("absNumeric(col2)", "abs_col"), ("money('NaN')", "nan_col")
    ]
    df = df.map(evals_list)
    evals_list_2 = [
        ("multNumeric('{}', div_col)".format(rand_decimal), "mult_col"),
        ("multNumeric(money('{}'), div_col)".format(rand_decimal),
         "mult_cast_col"),
        ("multNumeric(-1, div_col)", "mult_neg_col"),
    ]
    df = df.map(evals_list_2)
    session.execute_dataflow(df, "final_table", is_async=False)
    tab = session.get_table("final_table")
    xcalar_money_pd = pd.DataFrame.from_dict(tab.records())
    tab.drop()

    # same operations on pandas dataframe
    decimal_val = Decimal(rand_decimal)
    source_pd = pd.DataFrame.from_dict(gen_dataset.records())
    source_pd.fillna(value=pd.np.nan, inplace=True)
    source_pd['col2'] = source_pd['col2'].apply(lambda x: Decimal(x))

    source_pd[evals_list[0][1]] = source_pd['col2'] + decimal_val
    source_pd[evals_list[1][1]] = source_pd['col2']
    source_pd[evals_list[2][1]] = source_pd['col2'].apply(
        lambda x: x if math.isnan(x) else decimal_val if x < decimal_val else Decimal('1')
    )
    source_pd[evals_list[3][1]] = source_pd['col2'].apply(
        lambda x: x == decimal_val if not math.isnan(x) else None)
    source_pd[evals_list[4][1]] = source_pd['col2'] != decimal_val
    source_pd[evals_list[5][1]] = source_pd['col2'].apply(
        lambda x: not math.isnan(x))    # if exists
    source_pd[evals_list[6][1]] = source_pd['col2'].apply(
        lambda x: math.isnan(x))    # isNull
    source_pd[evals_list[7][1]] = source_pd['col2'] - decimal_val
    source_pd[evals_list[8][1]] = source_pd['col2'] / decimal_val
    source_pd[evals_list[9][1]] = source_pd['col2'].apply(
        lambda x: x if math.isnan(x) else isinstance(x, Decimal)
    )    # isNumeric
    source_pd[evals_list[10][1]] = source_pd['col1'].apply(
        lambda x: isinstance(x, Decimal))
    source_pd[evals_list[11][1]] = source_pd['col2'] == decimal_val
    source_pd[evals_list[12][1]] = source_pd['col2'].apply(
        lambda x: x if math.isnan(x) else True if x < decimal_val else False)
    source_pd[evals_list[13][1]] = source_pd['col2'].apply(
        lambda x: x if math.isnan(x) else True if x <= decimal_val else False)
    source_pd[evals_list[14][1]] = source_pd['col2'].apply(
        lambda x: x if math.isnan(x) else True if x > decimal_val else False)
    source_pd[evals_list[15][1]] = source_pd['col2'].apply(
        lambda x: x if math.isnan(x) else True if x >= decimal_val else False)
    source_pd[evals_list[16][1]] = source_pd['col2'].apply(
        lambda x: con.abs(x))
    source_pd[evals_list[17][1]] = Decimal('NaN')

    source_pd[evals_list_2[0][1]] = source_pd['div_col'].apply(
        lambda x: con.multiply(decimal_val, x))
    source_pd[evals_list_2[1][1]] = source_pd['div_col'].apply(
        lambda x: con.multiply(decimal_val, x))
    source_pd[evals_list_2[2][1]] = source_pd['div_col'] * -1

    # now compare xcalar results and pandas results
    num_columns = len(evals_list) + len(evals_list_2) + 2
    assert source_pd.shape == xcalar_money_pd.shape == (num_dataset_rows,
                                                        num_columns)
    # compare columns
    source_pd.sort_index(axis=1, inplace=True)
    xcalar_money_pd.sort_index(axis=1, inplace=True)
    assert (source_pd.columns == xcalar_money_pd.columns).all()

    # compare cell by cell value
    source_pd_sorted = source_pd.sort_values(by="col1").reset_index(drop=True)
    xcalar_pd_sorted = xcalar_money_pd.sort_values(by="col1").reset_index(
        drop=True)

    decimal_quantizer = '0.' + '0' * (money_scale - 1) + '1'

    for source_rec, xcalar_rec in zip(source_pd_sorted.get_values(),
                                      xcalar_pd_sorted.get_values()):
        assert len(source_rec) == len(xcalar_rec) == num_columns
        col_idx = -1
        for source_cell, xcalar_cell in zip(source_rec, xcalar_rec):
            col_idx += 1
            if isinstance(source_cell, Decimal):
                if source_cell.is_nan():
                    assert xcalar_cell == 'NaN' or math.isnan(xcalar_cell)
                else:
                    rounded_src_val = Decimal(
                        source_cell.quantize(
                            Decimal(decimal_quantizer),
                            rounding=ROUND_HALF_UP))
                    assert rounded_src_val.compare(
                        Decimal(xcalar_cell)) == Decimal(
                            '0'), "Invalid value for column {}".format(
                                source_pd.columns[col_idx])
            elif (source_cell is None
                  or math.isnan(source_cell)) and math.isnan(xcalar_cell):
                continue
            else:
                assert source_cell == xcalar_cell or source_cell - xcalar_cell == 0, "Invalid value for column {}".format(
                    source_pd.columns[col_idx])
