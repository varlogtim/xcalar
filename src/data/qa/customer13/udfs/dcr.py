# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

def isMonthFlag(start, end, month):
    return (int(start) <= int(month) <= int(end))

def concatEntireString(col1, col2, col3, col4, col5, col6):
    return str(col1) + "|" + str(col2) + "|" + str(col3) + "|" + str(col4) + "|" + str(col5) + "|" + str(col6)

def t_qc_amount(ioflag, iotype, amount):
    if (ioflag == "1" or (ioflag == "10" and iotype == "1")):
        return amount
    return str(-1 * float(amount))

def concatEntireString7(col1, col2, col3, col4, col5, col6, col7):
    return str(col1) + "|" + str(col2) + "|" + str(col3) + "|" + str(col4) + "|" + str(col5) + "|" + str(col6) + "|" + str(col7)

def doubleCase(a_amount, b_amount, a_sale):
    if (float(a_amount) > float(b_amount)):
        if (a_amount != ""):
            return (float(a_sale)/float(a_amount) * float(b_amount))
        return ""
    else:
        return a_sale

def xs_jh2_qc_amount(a_amount, c_amount):
    if (float(a_amount) > float(c_amount)):
        return str(float(a_amount) - float(c_amount))
    return ""

def gen_kcsku(ioflag, iotype, amount, code, colorid):
    if (ioflag == "111" and iotype == "111" and float(amount) > 0):
        return code + colorid
    else:
        return ""

def gen_kc(ioflag, iotype, amount):
    if (ioflag == "111" and iotype == "111"):
        return amount
    return ""

def gen_kcdpje(ioflag, iotype, amount, sale):
    if (ioflag == "111" and iotype == "111"):
        return str(float(amount) * float(sale))
    return ""

def gen_dxsku(ioflag, iotype, amount, code, colorid):
    if (ioflag == "112" and iotype == "112" and float(amount) > 0):
        return code + colorid
    else:
        return ""

def gen_dx(ioflag, iotype, amount):
    if (ioflag == "112" and iotype == "112"):
        return amount
    return ""

def gen_dxje(ioflag, iotype, amount, sale):
    if (ioflag == "112" and iotype == "112"):
        return str(float(amount) * float(sale))
    return ""

def addDelimiter(kcsku):
    if len(kcsku) > 0:
        return kcsku + "|"
    return ""

def countUniqueSku(sku):
    allSku = sku.split("|")
    skuDict = {}
    for s in allSku:
        if not s in skuDict:
            skuDict[s] = 1
    return str(len(skuDict))