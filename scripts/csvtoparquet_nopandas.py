import pyarrow as pa
from pyarrow import parquet
import csv
import optparse
import sys

def get_var(val, colid, trial, data_types):
    if not trial:
        if isinstance(data_types[colid], int):
            return int(val)
        elif isinstance(data_types[colid], float):
            return float(val)
        elif isinstance(data_types[colid], bool):
            vlower = val.lower()
            if vlower == "true":
                v = True
            elif vlower == "false":
                v = False
            return v
        elif isinstance(data_types[colid], str):
            return val

    v = None
    try:
        v = int(val)
    except:
        try:
            v = float(val)
        except:
            vlower = val.lower()
            if vlower == "true":
                v = True
            elif vlower == "false":
                v = False
            else:
                v = val
    return v

def get_table(csv_file, newline, delimiter, quotechar, trial, data_types, pa_data_types):
    fields = []
    values = []

    with open(csv_file, newline=newline) as csvfile:
        csvreader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
        num_cols = 0
        for row_id, row in enumerate(csvreader):
            if row_id == 0:
                header = row
                print(f"header = {header}")
                for _ in header:
                    values.append([])
                num_cols = len(header)
                continue
            if len(row) != num_cols:
                raise ValueError("Unbalanced columns")
            rowvalues = []
            for _ in header:
                rowvalues.append([])
            ICV = False
            for colid, col in enumerate(row):
                val = get_var(col, colid, trial, data_types)
                if isinstance(val, int):
                    if row_id == 1:
                        fields.append(pa.field(header[row_id], pa.int64()))
                        if trial:
                            data_types.append(int)
                            pa_data_types.append(pa.int64())
                    else:
                        if trial and data_types[colid] != int:
                            ICV = True
                            data_types[colid] = str
                            pa_data_types[colid] = pa.string()
                            break
                    rowvalues[colid].append(val) 
                elif isinstance(val, float):
                    if row_id == 1:
                        fields.append(pa.field(header[row_id], pa.float64()))
                        if trial:
                            data_types.append(float)
                            pa_data_types.append(pa.float64())
                    else:
                        if trial and data_types[colid] != float:
                            ICV = True
                            data_types[colid] = str
                            pa_data_types[colid] = pa.string()
                            break
                    rowvalues[colid].append(val) 
                elif isinstance(val, bool):
                    if row_id == 1:
                        fields.append(pa.field(header[row_id], pa.bool_()))
                        if trial:
                            data_types.append(bool)
                            pa_data_types.append(pa.bool_())
                    else:
                        if trial and data_types[colid] != bool:
                            ICV = True
                            data_types[colid] = str
                            pa_data_types[colid] = pa.string()
                            break
                    rowvalues[colid].append(val) 
                elif isinstance(val, str):
                    if row_id == 1:
                        fields.append(pa.field(header[row_id], pa.string()))
                        if trial:
                            data_types.append(str)
                            pa_data_types.append(pa.string())
                    else:
                        if trial and data_types[colid] != str:
                            ICV = True
                            data_types[colid] = str
                            pa_data_types[colid] = pa.string()
                            break
                    rowvalues[colid].append(val) 
                else:
                    raise ValueError(f"type of {val} is {type(val)}")
            if not ICV:
                for ind,_ in enumerate(values):
                    values[ind] += rowvalues[ind]
    
    #print(f"num arrays = {len(values)}, num fields = {len(fields)}")
    pavalues = []
    for colid,value in enumerate(values):
        #pavalues.append(pa.array(value, pa_data_types[colid]))
        pavalues.append(pa.array(value))
    return pavalues, fields, data_types, pa_data_types, ICV

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option('-f', '--csvfile', action="store", dest="csvfile", help="csv file path")
    parser.add_option('-o', '--outputfile', action="store", dest="outputfile", help="parquet output file")
    parser.add_option('-n', '--newline-delimiter', action="store", default='\n', dest="newline_delimiter", help="newline delimiter")
    parser.add_option('-d', '--field-delimiter', action="store", default=',', dest="field_delimiter", help="field delimiter")
    parser.add_option('-q', '--quotechar', action="store", default='"', dest="quotechar", help="quote char")
    options, args = parser.parse_args()
    if not options.csvfile and not options.outputfile:
        parser.print_help()
        sys.exit(1)

    pavalues, fields, data_types, pa_data_types, ICV = get_table(csv_file=options.csvfile, newline=options.newline_delimiter, delimiter=options.field_delimiter, quotechar=options.quotechar, trial=True, data_types=[], pa_data_types=[])

    pavalues, fields, data_types, pa_data_types, ICV = get_table(csv_file=options.csvfile, newline=options.newline_delimiter, delimiter=options.field_delimiter, quotechar=options.quotechar, trial=False, data_types=data_types, pa_data_types=pa_data_types)

    #print(f"num arrays = {len(pavalues)}, num fields = {len(fields)}")
    table = pa.Table.from_arrays(pavalues, schema=pa.schema(fields))
    #print(dir(pa))
    #print(pa.__version__)
    pa.parquet.write_table(table, options.outputfile)
