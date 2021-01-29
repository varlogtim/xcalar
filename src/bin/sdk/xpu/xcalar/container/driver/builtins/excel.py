import tempfile

import xcalar.container.driver.base as driver
import xcalar.container.context as ctx

import xlwt

# Set up logging
logger = ctx.get_logger()


# disabled until we decide if we want to include excel as a builtin driver
# @driver.register_export_driver(name="excel", is_builtin=True)
@driver.param(name="file_path", type=driver.STRING, desc="exported file path")
@driver.param(
    name="target",
    type=driver.TARGET,
    desc="target location to store the exported excel data")
def driver(table, file_path, target):
    columns = [c["columnName"] for c in table.columns()]
    xpu_id = ctx.get_xpu_id()
    if xpu_id != 0:
        return

    logger.debug(f"exporting table with xdbid {table._xdb_id} to {file_path}")

    with tempfile.TemporaryFile() as f:
        book = xlwt.Workbook()
        sheet1 = book.add_sheet('Sheet 1')
        for col_num, col_meta in enumerate(table.columns()):
            sheet1.write(0, col_num, col_meta["headerAlias"])
        for row_num, row in enumerate(table.all_rows()):
            for col_name, col in row.items():
                if col_name in columns:
                    col_num = columns.index(col_name)
                    sheet1.write(row_num + 1, col_num, str(col))
        sheet1.flush_row_data()
        book.save(f)
        f.seek(0)
        with open(file_path, "wb") as real_f:
            real_f.write(f.read())
