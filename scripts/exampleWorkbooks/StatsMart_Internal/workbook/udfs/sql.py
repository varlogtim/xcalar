# PLEASE TAKE NOTE:
# 
# Scalar function works on one or more
# fields of a table row
# and/or on literal values.
# Function automatically
# applys to all rows of a
# table.
# 
# Function def:
# 'def' NAME parameters ':'
#     [TYPE_COMMENT]
#     func_body_suite
# full grammar here: https://docs.python.org/3.6/reference/grammar.html
# Note: Return type is always
# treated as string
# 
# ex:
# def scalar_sum(col1, col2):
#     return col1 + col2;
# 
def sampleAddOne(col1):
    return col1 + 1
