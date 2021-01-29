# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

# Dummy function to make upload work (qgraph references sql:geDate, and sysymd)
def sysymd():
    return "dummy_date"

def geDate(date1, date2):
    return str(date1)
