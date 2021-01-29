# PLEASE TAKE NOTE: 

# UDFs can only support
# return values of 
# type String.

# Function names that 
# start with __ are
# considered private
# functions and will not
# be directly invokable.

def addOne(f):
    return str((int(f)+1)/0) # div by 0 to introduce failure
