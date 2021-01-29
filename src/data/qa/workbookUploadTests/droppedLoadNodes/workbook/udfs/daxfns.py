# PLEASE TAKE NOTE:

# UDFs can only support
# return values of
# type String.

# Function names that
# start with __ are
# considered private
# functions and will not
# be directly invokable.


# DATETIME FUNCTIONS:
# https://msdn.microsoft.com/en-us/library/ee634786.aspx

from dateutil.parser import parse
from datetime import time as Time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce
import math
import random
from scipy.stats import beta, chi2, expon, poisson, norm
defaultDateFormat = "%-m/%-d/%Y"

# Converts a datetimeObj to string
# dateTimeObj: date time object or a string that can be parsed as a datetimeobject
# date time format for strftime function of datetime class


def _formatDate(dateTimeObj, format="%Y-%m-%d"):
    return _get_date_time(dateTimeObj).strftime(format)

# if given number is odd


def _isodd(n):
    n = __builtins__['int'](n)
    return (n % 2) == 1

# if given number is even


def _iseven(n):
    n = __builtins__['int'](n)
    return (n % 2) == 0

# returns string representaiton of param object if applicable.
# if nocast == true bounce it back


def _cast_return(param, dateTimeFormat="", nocast=False):
    if(nocast):
        return param
    if(dateTimeFormat != ""):  # caller should make sure that str can be parsed as date
        return _get_date_time(param).strftime(dateTimeFormat)
    return str(param)

# returns a datetime object for given param
# param can be a datetime object itself.If this is the case function simply bounces it back
# otherwise it treats the param as a valid date time string and try to parse it
# missing parts of the object is filled with now ,
# for example "12:35 AM" will return a full datetime object with todays
# date and year


def _get_date_time(param):
    if isinstance(param, datetime):
        return param
    else:
        return parse(param)

# EDATE(<start_date>, <months>)
# Returns the date that is the indicated number of months before or after the start date.
# Use EDATE to calculate maturity dates or due dates that fall on the same day of the month as the date of issue
# start_date    A date in datetime or text format that represents the start date.
# months  An integer that represents the number of months before or after
# start_date.


def edate(start_date, months):
    return _cast_return(_get_date_time(start_date) +
                        relativedelta(months=+months), defaultDateFormat)

# EOMONTH(<start_date>, <months>)
# Returns the date in datetime format of the last day of the month, before or after a specified number of months.
# start_date    The start date in datetime format, or in an accepted text representation of a date.
# months  A number representing the number of months before or after the
# start_date. Note: If you enter a number that is not an integer, the
# number is rounded up or down to the nearest integer.


def eomonth(start_date, months):
    months = __builtins__['int'](months)
    edate = _get_date_time(start_date).replace(
        day=1) + relativedelta(months=+months)
    return _cast_return(edate + relativedelta(months=1,
                                              days=-1), defaultDateFormat)

# Returns the month as a number from 1 (January) to 12 (December).
# MONTH(<datetime>)
# Parameters:
# Term  Definition
# date  A date in datetime or text format.
# Return Value
# An integer number from 1 to 12


def month(date_time):
    return _cast_return(_get_date_time(date_time).month)

# HOUR(<param>)
# Returns the hour as a number from 0 (12:00 A.M.) to 23 (11:00 P.M.).
# param A datetime value, such as 16:48:00 or 4:48 PM


def hour(param):
    date_obj = param
   # date_time = datetime.date(2018,1, 1)
   # strftime('%Y-%m-%d %I:%M:%S %p')
    return _cast_return(_get_date_time(param).hour)

# MINUTE(<datetime>)
# Returns the minute as a number from 0 to 59, given a date and time value.
# date_time       A datetime value or text in an accepted time format,
# such as 16:48:00 or 4:48 PM.


def minute(date_time):
    return _cast_return(_get_date_time(date_time).minute)

# SECOND(<datetime>)
# Returns the second as a number from 0 to 59, given a date and time value.
# date_time       A datetime value or text in an accepted time format,
# such as 16:48:00 or 4:48 PM.


def second(date_time):
    return _cast_return(_get_date_time(date_time).second)

# Returns the current date and time in datetime format.


def now():
    return _cast_return(datetime.now())

# TIME(hour, minute, second)
# Converts hours, minutes, and seconds given as numbers to a time in datetime format.
# hour  A number from 0 to 23 representing the hour.
# Any value greater than 23 will be divided by 24 and the remainder will be treated as the hour value.
# minute    A number from 0 to 59 representing the minute.
# Any value greater than 59 will be converted to hours and minutes.
# second    A number from 0 to 59 representing the second.
# Any value greater than 59 will be converted to hours, minutes, and seconds.


def time(h, m, s):
    h = __builtins__['int'](h)
    m = __builtins__['int'](m)
    s = __builtins__['int'](s)
    print(h, m, s)
    # return _cast_return(datetime.time(h, m, s))

# TIMEVALUE(time_text)
# time_text A text string that that represents a certain time of the day.
# Any date information included in the time_text argument is ignored.


def timeValue(time_text):
    format = "%H:%M"
    if (time_text.count(':') == 2):
        format = format + ":%S"
    if("AM" in time_text or "PM" in time_text or "am" in time_text or "pm" in time_text):
        format = format + " %p"
    return _cast_return(datetime.strptime(time_text, format))

# TODAY()
# Returns the current date.


def today():
    return _cast_return(datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0), defaultDateFormat)

# UTCNOW()
# Returns the current UTC date and time


def utcNow():
    return _cast_return(datetime.utcnow())

# UTCTODAY()
# Returns the current UTC date.
# Return Value
# A date.
# UTCTODAY returns the time value 12:00:00 PM for all dates.
# The UTCNOW function is similar but returns the exact time and date.


def utcToday():
    return _cast_return(datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0))

# WEEKDAY(<date>, <return_type>)
# Returns a number from 1 to 7 identifying the day of the week of a date. By default the day ranges from 1 (Sunday) to 7 (Saturday).
# date  A date in datetime format.
# Dates should be entered by using the DATE function, by using expressions that result in a date, or as the result of other formulas.
# return_type   A number that determines the return value:
# Return type: 1, week begins on Sunday (1) and ends on Saturday (7). numbered 1 through 7.
# Return type: 2, week begins on Monday (1) and ends on Sunday (7).
# Return type: 3, week begins on Monday (0) and ends on Sunday
# (6).numbered 1 through 7.


def weekDay(date, return_type):
    return_type = __builtins__['int'](return_type)
    wDay = _get_date_time(date).weekday()  # Sunday is 6
    rv = None
    if (return_type == 1):
        rv = (wDay + 1) % 7 + 1
    elif (return_type == 2):
        rv = (wDay + 1)
    else:
        rv = wDay
    return _cast_return(rv)

# WEEKNUM(<date>, <return_type>)
# Returns the week number for the given date and year according to the return_type value.
# Return Value:
# An integer number.


def weekNum(date):
    return _cast_return(_get_date_time(date).isocalendar())

# BLANK()
# returns blank string


def blank():
    return ""

# CODE Function (DAX)
# Returns a numeric code for the first character in a text string.
# The returned code corresponds to the character set used by your computer.


def code(ch):
    return _cast_return(ord(ch))


# COMBINEVALUES(<delimiter>, <expression>, <expression>[, <expression>]…)
# The COMBINEVALUES function joins two or more text strings into one text string.
# The primary purpose of this function is to support multi-column relationships in DirectQuery models
# delimiter A separator to use during concatenation. Must be a constant value.
# expression    A DAX expression whose value will be be joined into a single text string.
# Return Value
# The concatenated string.
def combineValues(delimiter, *expressions):
    rv = ""
    for expression in expressions:
        rv = rv + expression + delimiter
    return rv[:-1]

# CONCATENATE(<text1>, <text2>)
# Joins two text strings into one text string.
# Parameters
# text1, text2    The text strings to be joined into a single text string.
# Strings can include text or numbers.


def concatenate(text1, text2):
    return text1 + text2


# EXACT(<text1>,<text2>)
# Compares two text strings and returns TRUE if they are exactly the same, FALSE otherwise.
# EXACT is case-sensitive but ignores formatting differences.
# You can use EXACT to test text being entered into a document.
# Parameters
# text1 The first text string or column that contains text.
# text2 The second text string or column that contains text
def exact(text1, text2):
    return _cast_return(text1 == text2)

# FIND(<find_text>, <within_text>[, [<start_num>][, <NotFoundValue>]])
# Returns the starting position of one text string within another text string. FIND is case-sensitive.
# Parameters
# find_text The text you want to find. Use double quotes (empty text) to match the first character in within_text.
# within_text   The text containing the text you want to find.
# start_num (optional) The character at which to start the search;
#   if omitted, start_num = 1. The first character in within_text is character number 1.
# NotFoundValue (optional) The value that should be returned when the operation does not find a matching substring,
#  typically 0, -1, or BLANK().


def find(find_text, within_text, start_num=0, NotFoundValue=-1):
    start_num = __builtins__['int'](start_num)
    rv = within_text.find(find_text, start_num)
    if (rv == -1):
        return _cast_return(NotFoundValue)
    else:
        return _cast_return(rv + 1)

# Rounds a number to the specified number of decimals and returns the result as text.
# You can specify that the result be returned with or without commas.
# Parameters
# number    The number you want to round and convert to text, or a column containing a number.
# decimals  (optional) The number of digits to the right of the decimal point; if omitted, 2.
# no_commas       (optional) A logical value: if 1, do not display commas
# in the returned text; if 0 or omitted, display commas in the returned
# text.


def fixed(number, decimals=2, no_commas=False):
    number = float(number)
    rv = __builtins__['round'](number, decimals)
    return _cast_return(rv)

# Returns the specified number of characters from the start of a text string.
# Syntax
# LEFT(<text>, <num_chars>)
# Parameters
# Term  Definition
# text  The text string containing the characters you want to extract, or a reference to a column that contains text.
# num_chars       (optional) The number of characters you want LEFT to
# extract; if omitted, 1.


def left(text, num_chars=1):
    num_chars = __builtins__['int'](num_chars)
    return text[:num_chars]

# LEN(<text>)
# Parameters
# Term  Definition
# text  The text whose length you want to find, or a column that contains text. Spaces count as characters.
# Return Value
# A whole number indicating the number of characters in the text string.


def len(text):
    return _cast_return(__builtins__['len'](text))

# Converts all letters in a text string to lowercase.
# Syntax
# LOWER(<text>)
# Parameters
# Term  Definition
# text    The text you want to convert to lowercase, or a reference to a
# column that contains text.


def lower(text):
    return text.lower()

# Returns a string of characters from the middle of a text string, given a starting position and length.
# Syntax
# MID(<text>, <start_num>, <num_chars>)
# Parameters
# Term  Definition
# text  The text string from which you want to extract the characters, or a column that contains text.
# start_num The position of the first character you want to extract. Positions start at 1.
# num_chars The number of characters to return.


def mid(text, start_num, num_chars):
    start_num = __builtins__['int'](start_num)
    num_chars = __builtins__['int'](num_chars)
    return text[start_num - 1: start_num + num_chars - 1]

# REPLACE replaces part of a text string, based on the number of characters you specify, with a different text string.
# Syntax
# REPLACE(<old_text>, <start_num>, <num_chars>, <new_text>)
# Parameters
# Term  Definition
# old_text  The string of text that contains the characters you want to replace, or a reference to a column that contains text.
# start_num The position of the character in old_text that you want to replace with new_text.
# num_chars The number of characters that you want to replace. Warning: If the argument, num_chars, is a blank or references a column that evaluates to a blank, the string for new_text is inserted at the position, start_num, without replacing any characters. This is the same behavior as in Excel.
# new_text  The replacement text for the specified characters in old_text.


def replace(old_text, start_num, num_chars, new_text):
    start_num = __builtins__['int'](start_num)
    num_chars = __builtins__['int'](num_chars)
    return old_text[:start_num - 1] + new_text + \
        old_text[start_num - 1 + num_chars:]

# Repeats text a given number of times. Use REPT to fill a cell with a number of instances of a text string.
# Syntax
# REPT(<text>, <num_times>)
# Parameters
# Term  Definition
# text  The text you want to repeat.
# num_times A positive number specifying the number of times to repeat text.
# Property Value/Return Value
# A string containing the changes.
# Remarks
# If number_times is 0 (zero), REPT returns a blank.
# If number_times is not an integer, it is truncated.
# The result of the REPT function cannot be longer than 32,767 characters,
# or REPT returns an error.


def rept(text, n):
    n = __builtins__['int'](n)
    return _cast_return(text * n)

# RIGHT returns the last character or characters in a text string, based on the number of characters you specify.
# Syntax
# RIGHT(<text>, <num_chars>)
# Parameters
# Term  Definition
# text  The text string that contains the characters you want to extract, or a reference to a column that contains text.
# num_chars (optional) The number of characters you want RIGHT to extract; is omitted, 1. You can also use a reference to a column that contains numbers.
# If the column reference does not contain text, it is implicitly cast as text.
# Property Value/Return Value
# A text string containing the specified right-most characters.


def right(text, num_chars=1):
    num_chars = __builtins__['int'](num_chars)
    return text[-(num_chars):]


# Returns the number of the character at which a specific character or text string is first found,
# reading left to right. Search is case-insensitive and accent sensitive.
# Syntax
# SEARCH(<find_text>, <within_text>[, [<start_num>][, <NotFoundValue>]])
# Parameters
# find_text The text that you want to find.
# You can use wildcard characters — the question mark (?) and asterisk (*) — in find_text. A question mark matches any single character; an asterisk matches any sequence of characters. If you want to find an actual question mark or asterisk, type a tilde (~) before the character.
# within_text   The text in which you want to search for find_text, or a column containing text.
# start_num (optional) The character position in within_text at which you want to start searching. If omitted, 1.
# NotFoundValue   (optional) The value that should be returned when the
# operation does not find a matching substring, typically 0, -1, or
# BLANK().

def search(find_text, within_text, start_num, NotFoundValue):
    start_num = __builtins__['int'](start_num)
    return ""

# Replaces existing text with new text in a text string.
# Syntax :
# SUBSTITUTE(<text>, <old_text>, <new_text>, <instance_num>)
# Parameters
# text  The text in which you want to substitute characters, or a reference to a column containing text.
# old_text  The existing text that you want to replace.
# new_text  The text you want to replace old_text with.
# instance_num  (optional) The occurrence of old_text you want to replace. If omitted, every instance of old_text is replaced
# Property Value/Return Value
# A string of text.


def substitute(text, old_text, new_text, instance_num=0):
    instance_num = __builtins__['int'](instance_num)
    if (instance_num != 0):
        return text.replace(old_text, new_text, instance_num)
    return text.replace(old_text, new_text)

# Removes all spaces from text except for single spaces between words.
# Syntax
# TRIM(<text>)
# Parameters
# text  The text from which you want spaces removed, or a column that contains text.
# Property Value/Return Value
# The string with spaces removed.


def trim(text):
    return text.strip()

# Returns the Unicode character referenced by the numeric value.
# Syntax
# UNICHAR(number)
# Parameters
# number    The Unicode number that represents the character.
# Return Value
# A character represented by the Unicode number


def unichar(number):
    number = __builtins__['int'](number)
    return str(chr(number))

# Converts a text string to all uppercase letters
# Syntax
# UPPER (<text>)
# Parameters
# Term  Definition
# text  The text you want converted to uppercase, or a reference to a column that contains text.
# Property Value/Return Value
# Same text, in uppercase.


def upper(text):
    return text.upper()

##### LOGICAL FUNCTIONS ####
# Checks whether both arguments are TRUE, and returns TRUE if both arguments are TRUE. Otherwise returns false.
# Syntax
# AND(<logical1>,<logical2>)
# Parameters
# logical_1,logical_2   The logical values you want to test.
# Return Value
# Returns true or false depending on the combination of values that you test.


def AND(logical1, logical2):
    logical1 = bool(logical1)
    logical2 = bool(logical2)
    return _cast_return(logical1 and logical2)

# Returns the logical value FALSE.
# Syntax
# FALSE() TRUE()
# Return Value
# Always FALSE/TRUE
# The word FALSE/TRUE is also interpreted as the logical value FALSE/TRUE.


def false():
    return _cast_return(False)


def true():
    return _cast_return(True)

# IF Function (DAX)
# Checks if a condition provided as the first argument is met.
# Returns one value if the condition is TRUE, and returns another value if the condition is FALSE.
# Syntax
# IF(<logical_test>,<value_if_true>, value_if_false)
# Parameters
# logical_test  Any value or expression that can be evaluated to TRUE or FALSE.
# value_if_true The value that is returned if the logical test is TRUE. If omitted, TRUE is returned.
# value_if_false    The value that is returned if the logical test is FALSE. If omitted, FALSE is returned.
# Return Value
# Any type of value that can be returned by an expression.


def IF(logical_test, value_if_true=True, value_if_false=False):
    logical_test = bool(logical_test)
    if (logical_test):
        return _cast_return(value_if_true)
    else:
        return _cast_return(value_if_false)

# IFERROR Function (DAX)
# Evaluates an expression and returns a specified value if the expression returns an error;
# otherwise returns the value of the expression itself.
# Syntax


def IFERROR(value, value_if_error):
    try:
        return _cast_return(eval(value))
    except BaseException:
        return _cast_return(value_if_error)

# NOT(<logical>)
# Changes FALSE to TRUE, or TRUE to FALSE.
# Parameters
# logical   A value or expression that can be evaluated to TRUE or FALSE.
# Return Value
# TRUE OR FALSE.


def NOT(logical):
    logical = bool(logical)
    return _cast_return(not logical)

# OR Function (DAX)
# Checks whether one of the arguments is TRUE to return TRUE. The function returns FALSE if both arguments are FALSE.
# Syntax
# OR(<logical1>,<logical2>)
# Return Value
# A Boolean value. The value is TRUE if any of the two arguments is TRUE;
# the value is FALSE if both the arguments are FALSE.


def OR(logical1, logical2):
    logical1 = bool(logical1)
    logical2 = bool(logical2)
    return _cast_return(logical1 or logical2)

### Date and Time Functions ###
# Returns a table with a single column named “Date” that contains a contiguous set of dates.
# The range of dates is from the specified start date to the specified end date, inclusive of those two dates.
# Syntax
# CALENDAR(<start_date>, <end_date>)
# Parameters
# Term  Definition
# start_date    Any DAX expression that returns a datetime value.
# end_date  Any DAX expression that returns a datetime value.
# Return Value
# Returns a table with a single column named “Date” containing a contiguous set of dates.
# The range of dates is from the specified start date to the specified end date, inclusive of those two dates.
# Not that Xcalar version of this function only takes scalar date values.


def calendar(start_date, end_date, delimiter=";"):
    start_date = _get_date_time(start_date)
    end_date = _get_date_time(end_date)
    d = start_date
    delta = timedelta(days=1)
    rv = []
    while d <= end_date:
        rv.append(_formatDate(d, defaultDateFormat))
        d += delta
    return delimiter.join(rv)


# Returns the specified date in datetime format.
def date(year, month, day):
    year = __builtins__['int'](year)
    month = __builtins__['int'](month)
    day = __builtins__['int'](day)
    return _cast_return(datetime(year, month, day), defaultDateFormat)


# Checks if a value is not text (blank cells are not text), and returns TRUE or FALSE.
# Syntax
# ISNONTEXT(<value>)
# Parameters
# Term  Definition
# value The value you want to check.
# Return Value
# TRUE if the value is not text or blank; FALSE if the value is text.
# Remarks
# An empty string is considered text.
def isNonText(value):
    if(isinstance(value, str)):
        if(__builtins__['len'](value) > 0):
            return _cast_return(False)
    return _cast_return(True)


# Checks if a value is text, and returns TRUE or FALSE.
# Syntax
# ISTEXT(<value>)
# Parameters
# Term  Definition
# value The value you want to check.
def isText(value):
    if(isinstance(value, str)):
        if(__builtins__['len'](value) > 0):
            return _cast_return(True)
    return _cast_return(False)


# Return Value
# Returns TRUE if number is odd, or FALSE if number is even.
# Remarks
# If number is nonnumeric, ISODD returns the #VALUE! error value.
# Syntax
# Return Value
def isOdd(param):
    param = __builtins__['int'](param)
    return _cast_return(_isodd(param))


def isEven(param):
    param = __builtins__['int'](param)
    return _cast_return(_iseven(param))

# Converts a date in the form of text to a date in datetime format.
# Syntax
# DATEVALUE(date_text)
# Parameters
# Term  Definition
# date_text Text that represents a date.
# Property Value/Return Value
# A date in datetime format.
# Remarks
# The DATEVALUE function uses the locale and date/time settings of the client computer
# to understand the text value when performing the conversion. If the current date/time settings represent dates
# in the format of Month/Day/Year, then the string, "1/8/2009", would be converted to a datetime value equivalent
# to January 8th of 2009. However, if the current date and time settings represent dates
# in the format of Day/Month/Year, the same string would be converted as a datetime value equivalent
# to August 1st of 2009.


def dateValue(date_text):
    return _cast_return(_get_date_time(date_text))

# If the year portion of the date_text argument is omitted, the DATEVALUE function uses the current year from your computer's built-in clock. Time information in the date_text argument is ignored.
# CALENDARAUTO Function
# DATE Function
# DATEDIFF Function
# DATEVALUE Function
# DAY Function
# Returns the day of the month, a number from 1 to 31.
# Syntax
# DAY(<date>)
# Parameters
# Term  Definition
# date  A date in datetime format, or a text representation of a date.


def day(param):
    return _cast_return(_get_date_time(param).day)

# Syntax
# YEAR(<date>)
# Parameters
# Term  Definition
# date  A date in datetime or text format, containing the year you want to find.
# Return Value
# An integer in the range 1900-9999.


def year(param):
    return _cast_return(_get_date_time(param).year)

### Math and Trig Functions ###
# ABS Function


def abs(param):
    param = float(param)
    return _cast_return(__builtins__['abs'](param))

# ACOS Function


def acos(param):
    param = float(param)
    return _cast_return(math.acos(param))

# ACOSH Function


def acosh(param):
    param = float(param)
    return _cast_return(math.acosh(param))

# ASIN Function


def asin(param):
    param = float(param)
    return _cast_return(math.asin(param))

# ASINH Function


def asinh(param):
    param = float(param)
    return _cast_return(math.asinh(param))

# ATAN Function


def atan(param):
    param = float(param)
    return _cast_return(math.atan(param))

# ATANH Function


def atanh(param):
    param = float(param)
    return _cast_return(math.atanh(param))
# CEILING Function
# CEILING(<number>, <significance>)
# Parameters
# Term  Definition
# number    The number you want to round, or a reference to a column that contains numbers.
# significance  The multiple of significance to which you want to round.
# For example, to round to the nearest integer, type 1.


def ceiling(number, significance):
    number = float(number)
    significance = float(significance)
    a = math.floor(number / significance) * significance
    if (a == number):
        return _cast_return(a)
    else:
        return _cast_return(a + significance)


# COMBIN Function
def combin(n, r):
    n = __builtins__['int'](n)
    r = __builtins__['int'](r)
    r = min(r, n - r)  # to get least number of multiplications in reduce
    # eliminates the need to calculate 3 factorials by removing common terms
    # in n! and (n - r)!
    return _cast_return(reduce(int.__mul__, list(
        range(n - r + 1, n + 1)), 1) / math.factorial(r))

# COMBINA Function


def combinA(n, r):
    n = float(n)
    r = float(r)
    new_n = n + r - 1
    new_r = n - 1
    # since mathematical formula of combA(n, r) is (n + r - 1)C(n - 1)
    return comb(new_n, new_r)

# COS Function


def cos(param):
    param = float(param)
    return _cast_return(math.cos(param))

# COSH Function


def cosh(param):
    return _cast_return(math.cosh(param))
# CURRENCY Function #NOT SUPPORTED#


def _truncate(number, digits):
    number = float(number)
    digits = __builtins__['int'](digits)
    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper


# Evaluates the argument and returns the result as currency data type.
# Syntax
# CURRENCY(<value>)
# Parameters
# value
# Any DAX expression that returns a single scalar value where the expression is
# to be evaluated exactly once before all other operations.
# Return Value
# The value of the expression evaluated and returned as a currency type value.
# Remarks
# The CURRENCY function rounds up the 5th significant decimal, in value, to return the 4th decimal digit;
# rounding up occurs if the 5th significant decimal is equal or larger than 5. For example,
# if value is 3.6666666666666 then converting to currency returns $3.6667; however,
# if value is 3.0123456789 then converting to currency returns $3.0123.
# If the data type of the expression is TrueFalse then
# CURRENCY( <TrueFalse>) will return $1.0000 for True values and $0.0000 for False values.
# If the data type of the expression is Text then CURRENCY(<Text>) will try to convert text to a number;
# if conversion succeeds the number will be converted to currency, otherwise an error is returned.
# If the data type of the expression is DateTime then CURRENCY(<DateTime>) will convert the datetime value
# to a number and that number to currency. DateTime values have an integer part that represents the
# number of days between the given date and 1900-03-01 and a fraction that represents the fraction of a day
# (where 12 hours or noon is 0.5 day). If the value of the expression is not a proper DateTime value
# an error is returned.
# Example
# Convert number 1234.56 to currency data type.
def currency(value):
    value = float(value)
    value = _truncate(value, 5)
    return format(__builtins__['round'](value, 5), ".4f")

# DEGREES Function


def degrees(param):
    param = float(param)
    return _cast_return(math.degrees(param))

# DIVIDE Function (DAX)
# Performs division and returns alternate result or BLANK() on division by 0.
# Syntax


def divide(numerator, denominator, alternateresult=blank()):
    numerator = float(numerator)
    denominator = float(denominator)
    if not (denominator == 0):
        return _cast_return(numerator / denominator)
    else:
        return alternateresult

# EVEN Function
# rounds up the param to closest even number


def even(param):
    param = float(param)
    floor = math.floor(param)
    if _isodd(floor):
        return _cast_return(floor + 1)
    else:
        return _cast_return(floor)

# EXP Function


def exp(param):
    param = float(param)
    return _cast_return(math.exp(param))

# FACT Function


def fact(param):
    param = float(param)
    return _cast_return(math.factorial(x))

# FLOOR Function


def floor(param):  # FIXME add signifignace
    param = float(param)
    return _cast_return(math.floor(param))
# GCD Function


def _gcd(a, b):
    a = __builtins__['int'](a)
    b = __builtins__['int'](b)
    while b:
        a, b = b, a % b
    return a


def _lcm(a, b):
    # gives lowest common multiple of two numbers
    a = __builtins__['int'](a)
    b = __builtins__['int'](b)
    return a * b // _gcd(a, b)


def gcd(*args):
    # gives LCM of a list of numbers passed as argument
    return _cast_return(reduce(_gcd, args))

# INT Function


def int(param):
    param = __builtins__['int'](param)
    if(param < 0):
        return _cast_return(param - 1)
    else:
        return _cast_return(param)

# ISO.CEILING Function


def iso_ceiling(number, significance):
    number = float(number)
    significance = __builtins__['int'](significance)
    return CEILING(number, significance)
# LCM Function


def lcm(*args):
    # gives LCM of a list of numbers passed as argument
    return _cast_return(reduce(_lcm, args))


# LN Function
def ln(param):
    param = float(param)
    return _cast_return(math.log(param))

# LOG Function


def log(number, base):
    number = float(number)
    base = __builtins__['int'](base)
    return _cast_return(math.log(number, base))

# LOG10 Function


def log10(param):
    param = float(param)
    return _cast_return(math.log(param, 10.0))

# MOD Function


def mod(number, divisor):
    number = float(number)
    divisor = float(divisor)
    return _cast_return(number % divisor)

# MROUND Function


def mround(number, multiple):
    number = float(number)
    multiple = float(multiple)
    return _cast_return(multiple * __builtins__['round'](number / multiple))


# ODD Function FIXME
def odd(param):
    param = float(param)
    floor = math.floor(param)
    if _iseven(floor):
        return _cast_return(floor + 1)
    else:
        if (floor == param):
            return _cast_return(floor)
        else:
            return _cast_return(floor + 2)

# PI Function


def pi():
    return _cast_return(math.pi)
# POWER Function


def power(number, power):
    number = float(number)
    power = float(power)
    return _cast_return(number ** power)

# QUOTIENT Function


def quotient(numerator, denominator):
    numerator = float(numerator)
    denominator = float(denominator)
    return _cast_return(__builtins__['int'](numerator / denominator))

# RADIANS Function
# Converts degrees to radians.
# Syntax
# RADIANS(angle)
# Parameters
# angle Required. An angle in degrees that you want to convert


def radians(angle):
    angle = float(angle)
    return _cast_return(math.radians(angle))

# RAND Function
# Returns a random number greater than or equal to 0 and less than 1, evenly distributed.
# The number that is returned changes each time the cell containing this
# function is recalculated.


def rand():
    return _cast_return(random.uniform(0, 1))

# RANDBETWEEN Function
# Returns a random number in the range between two numbers you specify.
# Syntax
# RANDBETWEEN(<bottom>,<top>)
# Parameters
# Bottom    The smallest integer the function will return.
# Top   The largest integer the function will return.
# Return Value
# A whole number.


def randBetween(bottom, top):
    bottom = float(bottom)
    top = float(top)
    return _cast_return(random.randrange(bottom, top))

# Rounds a number to the specified number of digits.
# Syntax
# ROUND(<number>, <num_digits>)
# Parameters
# Term  Definition
# number    The number you want to round.
# num_digits    The number of digits to which you want to round.
# A negative value rounds digits to the left of the decimal point;
# a value of zero rounds to the nearest integer.


def round(number, num_digits):
    number = float(number)
    num_digits = __builtins__['int'](num_digits)
    return _cast_return(__builtins__['round'](number, num_digits))


# ROUNDDOWN Function (DAX)
# Rounds a number down, toward zero.
# Syntax
# ROUNDDOWN(<number>, <num_digits>)
# Parameters
# Term  Definition
# number    A real number that you want rounded down.
# num_digits    The number of digits to which you want to round. Negative rounds to the left of the decimal point; zero to the nearest integer.
# Return Value
# A decimal number.

def roundDown(number, num_digits, decimalSeperator="."):
    number = float(number)
    num_digits = __builtins__['int'](num_digits)
    numberStr = str(number)
    tokens = numberStr.split(decimalSeperator)
    if(__builtins__['len'](tokens) == 1):
        tokens.append("")
        decimalSeperator = ""
    if(num_digits <= 0):
        rv = tokens[0][:__builtins__['len'](
            tokens[0]) + num_digits] + "0" * (-1 * num_digits)
    else:
        rv = tokens[0] + decimalSeperator + tokens[1][0:num_digits]
    return rv

# Rounds a number up, away from 0 (zero).
# ROUNDUP(<number>, <num_digits>)
# Parameters
# Term  Definition
# number    A real number that you want to round up.
# num_digits    The number of digits to which you want to round. A negative value for num_digits rounds to the left of the decimal point; if num_digits is zero or is omitted, number is rounded to the nearest integer.
# Return Value
# A decimal number.
# Remarks
# ROUNDUP behaves like ROUND, except that it always rounds a number up.
# If num_digits is greater than 0 (zero), then the number is rounded up to the specified number of decimal places.
# If num_digits is 0, then number is rounded up to the nearest integer.
# If num_digits is less than 0, then number is round


def roundUp(number, num_digits, decimalSeperator="."):
    if(decimalSeperator in str(num_digits)):
        rv = (
            __builtins__['int'](
                float(number) / float(num_digits))) * num_digits
        if(rv < float(number)):
            return _cast_return((__builtins__['int'](
                float(number) / float(num_digits)) + 1) * float(num_digits))
        return _cas_return(rv)
# Returns the square root of a number.
# Syntax
# SQRT(<number>)
# Parameters
# Term  Definition
# number    The number for which you want the square root, a column that contains numbers, or an expression that evaluates to a number.
# Return Value
# A decimal number.


def sqrt(number):
    number = float(number)
    return _cast_return(number ** 0.5)

# TRUNC Function
# Truncates a number to an integer by removing the decimal, or fractional, part of the number.
# Syntax
# TRUNC(<number>,<num_digits>)
# Parameters
# Term  Definition
# number    The number you want to truncate.
# num_digits    A number specifying the precision of the truncation; if omitted, 0 (zero)
# Return Value
# A whole number.


def trunc(number, num_digits=0):
    number = float(number)
    num_digits = __builtins__['int'](num_digits)
    stepper = pow(10.0, num_digits)
    if (num_digits > 0):
        return _cast_return(math.trunc(stepper * number) / stepper)
    else:
        return _cast_return(math.trunc(stepper * number))

# BETA.DIST Function (DAX)
# Returns the beta distribution.
# The beta distribution is commonly used to study variation in the percentage of something across samples,
# such as the fraction of the day people spend watching television.
# Syntax
# BETA.DIST(x,alpha,beta,cumulative,[A],[B])
# Parameters
# Term  Definition
# x The value between A and B at which to evaluate the function
# Alpha A parameter of the distribution.
# Beta  A parameter of the distribution.
# A Optional. A lower bound to the interval of x.
# B Optional. An upper bound to the interval of x.
# Return Value
# Returns the beta distribution.


def _betaPdf(x, a, b, loc=0, scale=1):
    x = float(x)
    a = float(a)
    b = float(b)
    loc = float(loc)
    scale = float(scale)
    return str(beta.pdf(x, a, b, loc, scale))


def _betaCdf(x, a, b, loc=0, scale=1):
    x = float(x)
    a = float(a)
    b = float(b)
    loc = float(loc)
    scale = float(scale)
    return str(beta.cdf(x, a, b, loc, scale))


def _betaPpf(p, a, b, loc=0, scale=1):
    p = float(p)
    a = float(a)
    b = float(b)
    loc = float(loc)
    scale = float(scale)
    return str(beta.ppf(p, a, b, loc, scale))


def beta_dist(x, alph, bet, cumulative=True, A=False, B=False):
    x = float(x)
    alph = float(alph)
    bet = float(bet)
    cumulative = bool(cumulative)
    A = bool(A)
    B = bool(B)
    if(A and B):    # check if A > x > B
        if (x < A or x > B or A == B):
            raise Exception
    if cumulative:
        return _cast_return(_betaCdf(x, alph, bet))
    return _cast_return(_betaPdf(x, alph, bet))


# Returns the inverse of the beta cumulative probability density function
# (BETA.DIST).

# If probability = BETA.DIST(x,...TRUE), then BETA.INV(probability,...) =
# x. The beta distribution can be used in project planning to model
# probable completion times given an expected completion time and
# variability.

# Syntax
# BETA.INV(probability,alpha,beta,[A],[B])
# Parameters
# Term  Definition
# Probability   A probability associated with the beta distribution.
# Alpha A parameter of the distribution.
# Beta  A parameter the distribution.
# A Optional. A lower bound to the interval of x.
# B Optional. An upper bound to the interval of x.
# Return Value
# Returns the inverse of the beta cumulative probability density function
# (BETA.DIST).

def beta_inv(x, alph, bet, A=False, B=False):
    x = float(x)
    alph = float(alph)
    bet = float(bet)
    A = bool(A)
    B = bool(B)
    if(A and B):    # check if A > x > B
        if (x < A or x > B or A == B):
            raise Exception
    return _cast_return(_betaPpf(x, alph, bet))


def _chisqPdf(x, degOfFreedom, loc=0, scale=1):
    x = float(x)
    degOfFreedom = float(degOfFreedom)
    loc = float(loc)
    scale = float(scale)
    return str(chi2.pdf(x, degOfFreedom, loc, scale))


def _chisqCdf(x, degOfFreedom, loc=0, scale=1):
    x = float(x)
    degOfFreedom = float(degOfFreedom)
    loc = float(loc)
    scale = float(scale)
    return str(chi2.cdf(x, degOfFreedom, loc, scale))


def _chisqPpf(p, degOfFreedom, loc=0, scale=1):
    p = float(p)
    degOfFreedom = float(degOfFreedom)
    loc = float(loc)
    scale = float(scale)
    return str(chi2.ppf(p, degOfFreedom, loc, scale))

# Returns the beta distribution. The beta distribution is commonly used to study variation in the percentage of something across samples, such as the fraction of the day people spend watching television.
# BETA.DIST(x,alpha,beta,cumulative,[A],[B])
# Parameters
# Term  Definition
# x The value between A and B at which to evaluate the function
# Alpha A parameter of the distribution.
# Beta  A parameter of the distribution.
# A Optional. A lower bound to the interval of x.
# B Optional. An upper bound to the interval of x.
# Return Value
# Returns the beta distribution.


def chisq_dist(x, alph, beta, cumulative, A, B):
    x = float(x)
    alph = float(alph)
    beta = float(beta)
    A = bool(A)
    B = bool(B)
    if(A and B):    # check if A > x > B
        if (x < A or x > B or A == B):
            raise Exception
    return _cast_return(_beta(x, alph, bet))

# Returns the inverse of the left-tailed probability of the chi-squared distribution.
# The chi-squared distribution is commonly used to study variation in the percentage of something across samples,
#  such as the fraction of the day people spend watching television.
# Syntax
# CHISQ.INV(probability,deg_freedom)
# Parameters
# Term  Definition
# Probability   A probability associated with the chi-squared distribution.
# Deg_freedom   The number of degrees of freedom.
# Return Value
# Returns the inverse of the left-tailed probability of the chi-squared
# distribution.


def chisq_inv(probability, deg_freedom):
    probability = float(probability)
    deg_freedom = float(deg_freedom)
    return _cast_return(_chisqPpf(1 - probability, deg_freedom))

# CONFIDENCE.T Function


def confidence_t(confidence_level, mean, std, size):
    confidence_level = float(confidence_level)
    mean = float(mean)
    std = float(std)
    size = float(size)
    return str(norm.interval(confidence_level, loc=mean,
                             scale=std / (size**0.5)))


# helper functions to implement EXPON.DIST
def _exponPdf(x, _lambda, loc=0, scale=1):
    x = float(x)
    _lambda = float(_lambda)
    loc = float(loc)
    scale = float(scale)
    return str(_lambda * expon.pdf(_lambda * x, loc, scale))


def _exponCdf(x, _lambda, loc=0, scale=1):
    x = float(x)
    _lambda = float(_lambda)
    loc = float(loc)
    scale = float(scale)
    return str(expon.cdf(_lambda * x, loc, scale))


def _exponPpf(p, _lambda, loc=0, scale=1):
    x = float(x)
    _lambda = float(_lambda)
    loc = float(loc)
    scale = float(scale)
    return str(expon.ppf(p, loc, scale) / _lambda)

# EXPON.DIST Function (DAX)
# Returns the exponential distribution. Use EXPON.DIST to model the time between events, such as how long an automated bank teller takes to deliver cash. For example,
# you can use EXPON.DIST to determine the probability that the process takes at most 1 minute.
# Syntax
# EXPON.DIST(x,lambda,cumulative)
# Parameters
# Term  Definition
# x Required. The value of the function.
# lambda    Required. The parameter value.
# cumulative    Required. A logical value that indicates which form of the exponential function to provide. If cumulative is TRUE, EXPON.DIST returns the cumulative distribution function; if FALSE, it returns the probability density function.
# Return Value
# Returns the exponential distribution.


def expon_dist(x, l, c):
    x = float(x)
    l = float(l)
    c = float(c)
    if c:
        return _cast_return(_exponCdf(x, l))
    else:
        return _cast_return(_exponPdf(x, l))


# Returns the Poisson distribution.
# A common application of the Poisson distribution is predicting the number of events over a specific time,
# such as the number of cars arriving at a toll plaza in 1 minute.
# Syntax
# POISSON.DIST(x,mean,cumulative)
# Parameters
# Term  Definition
# x Required. The number of events.
# mean  Required. The expected numeric value.
# cumulative    Required. A logical value that determines the form of the probability distribution returned. If cumulative is TRUE, POISSON.DIST returns the cumulative Poisson probability that the number of random events occurring will be between zero and x inclusive; if FALSE, it returns the Poisson probability mass function that the number of events occurring will be exactly x.
# Return Value
# Returns the Poisson distribution.

def _poissonPdf(k, mu, loc=0):
    k = float(k)
    mu = float(mu)
    loc = float(loc)
    return str(poisson.pmf(k, mu, loc))


def _poissonCdf(k, mu, loc=0):
    k = float(k)
    mu = float(mu)
    loc = float(loc)
    return str(poisson.cdf(k, mu, loc))


def _poissonPpf(p, mu, loc=0):
    p = float(p)
    mu = float(mu)
    loc = float(loc)
    return str(poisson.ppf(p, mu, loc))


def poisson_dist(x, mean, cumulative):
    x = float(x)
    mean = float(mean)
    cumulative = bool(cumulative)
    if cumulative:
        return _cast_return(_poissonCdf(x, mean))
    return _cast_return(_poissonPdf(x, mean))

# SIN Function


def sin(param):
    param = float(param)
    return _cast_return(math.sin(param))

# Returns the hyperbolic sine of a number.
# Syntax
# SINH(number)
# Parameters
# Term  Definition
# number    Required. Any real number.
# Return Value
# Returns the hyperbolic sine of a number.


def sinh(number):
    number = float(number)
    return _cast_return(math.sinh(number))

# Syntax
# SQRTPI(number)
# Parameters
# Term  Definition
# number    Required. The number by which pi is multiplied.
# Return Value
# Returns the square root of (number * pi).


def sqrtpi(number):
    number = float(number)
    return _cast_return((number * math.pi) ** 0.5)


# Returns the tangent of the given angle.
# TAN(number)
# Parameters
# Term  Definition
# number    Required. The angle in radians for which you want the tangent.

def tan(number):
    number = float(number)
    return _cast_return(math.tan(number))


from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from calendar import isleap as __isleap
import math
from functools import reduce


defaultDateFmt = "%m/%d/%Y"
defaultTimeFmt = "%H:%M:%S"

###
#
# Streaming UDF's for datetime
#
###

# get generator for all dates between start and end included
# start, end: must be of type Date


def __rangeOfDates(start, end):
    for n in range(__builtins__['int']((end - start).days) + 1):
        yield start + timedelta(n)

# return format to Xcalar design


def __toDate(date, columnName, inputFormat, index):
    return {
        columnName: date.strftime(inputFormat),
        "Row": index
    }

# use when startDate and endDate are explicit dates
# generates a two column table with the following structure:
#    <DateColumn>: all dates between startDate and endDate
#    Row: contains numbers next to the dates in ascending order,
#        used to sort the dates in Xcalar


def calenderExplicit(filePath, inStream, startDate="01/01/1996",
                     endDate="24/08/2005", inputFormat=defaultDateFmt,
                     columnName="Date"):
    start_date = datetime.strptime(startDate, inputFormat)
    end_date = datetime.strptime(endDate, inputFormat)
    for i, date in enumerate(__rangeOfDates(start_date, end_date)):
        yield __toDate(date, columnName, inputFormat, i)

# given the column number of the date column, returns the earliest
# and latest dates in that column


def __get_dates(inStream, delimiter, dateColNo, inputFormat):
    minDate = None
    maxDate = None
    for line in inStream:
        date_str = line.split(delimiter)[dateColNo]
        try:
            curr_date = datetime.strptime(date_str, inputFormat)
            if minDate is None or curr_date < minDate:
                minDate = curr_date
            if maxDate is None or curr_date > maxDate:
                maxDate = curr_date
        except BaseException:
            continue
    return (minDate, maxDate)

# use when startDate or endDate are to be inferred from data
# generates a two column table with the following structure:
#    <DateColumn>: all dates between earliest date in column and
#        last date in column, unless explicit start or end date
#        specified in which case that is used.
#    Row: contains numbers next to the dates in ascending order,
#        used to sort the dates in Xcalar


def calendarMinmax(filePath, inStream, delimiter='\t',
                   dateColNo=0, inputFormat=defaultDateFmt,
                   startDate=None, endDate=None,
                   outputFormat=None, columnName="Date"):
    # keep same formating for output if none given
    outputFormat = outputFormat or inputFormat
    if startDate and endDate:
        start_date = datetime.strptime(startDate, inputFormat)
        end_date = datetime.strptime(endDate, inputFormat)
    else:
        dates = __get_dates(inStream, delimiter, dateColNo,
                            inputFormat)
        start_date = (datetime.strptime(startDate, inputFormat)
                      if startDate else dates[0])
        end_date = (datetime.strptime(endDate, inputFormat)
                    if endDate else dates[1])
    for i, date in enumerate(__rangeOfDates(start_date, end_date)):
        yield __toDate(date, columnName, outputFormat, i + 1)

###
#
# UDF's for datetime
#
###

# convert dates to a default string format, so operations can run
# without inputFormat being explicitly provided


def toDefaultDate(dateStr, inputFormat):
    return datetime.strptime(dateStr, inputFormat).strftime(defaultDateFmt)

# convert time to a default string format, so operations can run
# without inputFormat being explicitly provided


def toDefaultTime(timeStr, inputFormat):
    return datetime.strptime(timeStr, inputFormat).strftime(defaultTimeFmt)

# def diff(startDate, endDate, intervalType = "days",
#         inputFormat = defaultDateFmt):
#    start_date = datetime.strptime(startDate, inputFormat)
#    end_date = datetime.strptime(endDate, inputFormat)


# types for which we know the exact difference
deterministicTypes = ["days", "hours", "minutes", "seconds"]
# returns the difference in date/time for two dates in the format specified
# acceptable: days, hours, minutes, seconds, years, months, quarters


def dateDiff(startDate, endDate, intervalType="days"):
    start_date = _get_date_time(startDate)
    end_date = _get_date_time(endDate)
    # currently returns integers, this can be modified by removing cast to
    # int for the deterministic types
    if intervalType in deterministicTypes:
        diff_tdelta = end_date - start_date
        if intervalType == "days":
            return str(diff_tdelta.days)
        total_seconds = diff_tdelta.total_seconds()
        if intervalType == "seconds":
            return str(__builtins__['int'](total_seconds))
        elif intervalType == "minutes":
            return str(__builtins__['int'](total_seconds / 60))
        elif intervalType == "hours":
            return str(__builtins__['int'](total_seconds / 3600))
    else:
        year_diff = end_date.year - start_date.year
        if intervalType == "years":
            return str(year_diff)
        month_diff = 12 * year_diff + end_date.month - start_date.month
        if intervalType == "months":
            return str(month_diff)
        # get quarter for both months and subtract the two. Cannot change
        # order of division and subtraction.
        quarter_diff = (4 * year_diff + (end_date.month - 1) / 3 -
                        (start_date.month - 1) / 3)
        if intervalType == "quarters":
            return str(quarter_diff)

# return the fraction of the year represented by the difference of two dates,
# with the modified day


def __calc360dayDiff(edate, sdate, eday, sday):
    duration = ((edate.year - sdate.year) * 360 +
                (edate.month - sdate.month) * 30 +
                (eday - sday))
    return float(duration) / 360


def __yf_usNasd(edate, sdate):
    # keep day the same if no condition for modification is true
    eday, sday = edate.day, sdate.day
    # if sdate is on last day of feb or is equal to 31
    if (((sdate + timedelta(1)).month == (sdate.month + 1))
            and sdate.day != 30):
        sday = 30
        # edate is only modified if sdate is modified and it
        # falls on the last day of the month
        if (edate + timedelta(1)).month == (edate.month + 1):
            # eday changes if sday and eday are on the last
            # day of feb or sday and eday are both 31
            if (edate.month == 2) == (sdate.month == 2):
                eday = 30
    return __calc360dayDiff(edate, sdate, eday, sday)


def __yf_actual(edate, sdate):
    return float((edate - sdate).days) / (365 + __isleap(sdate.year))


def __yf_actual360(edate, sdate):
    return float((edate - sdate).days) / 360


def __yf_actual365(edate, sdate):
    return float((edate - sdate).days) / 365


def __yf_eu360(edate, sdate):
    eday, sday = min(edate.day, 30), min(sdate.day, 30)
    return __calc360dayDiff(edate, sdate, eday, sday)


yf_fnMap = [
    __yf_usNasd,
    __yf_actual,
    __yf_actual360,
    __yf_actual365,
    __yf_eu360]
# Return the fraction of an year represented by the difference between start
# and end dates with the specified format given in the arg


def yearFrac(startDate, endDate, basis=0):
    start_date = _get_date_time(startDate)
    end_date = _get_date_time(end_date)
    # pass computation to specific function based on inputFormat
    # (Python's equivalent of a simple switch - case)
    return str(yf_fnMap[basis](end_date, start_date))


def isBlank(val):
    return _cast_return(str(val) == "")


def isError(val):
    return val is None


def isLogical(val):
    strVal = str(val)
    if strVal.upper() == "TRUE" or strVal.upper() == "FALSE":
        return _cast_return(True)
    else:
        return _cast_return(False)


def isNumber(n):
    if isinstance(n, bool):
        return str(False)
    try:
        n = float(n)
        return str(True)
    except BaseException:
        return str(False)

# if there is an error in the conditional, returns outVal


def ifError(cond, ifTrue, ifFalse):
    # FNF, Xcalar's error message for cols is passed
    # as None to python.
    if (cond is None):
        return str(ifTrue)
    else:
        return ifFalse

# SWITCH Function (DAX)
# Evaluates an expression against a list of values and returns one of multiple possible result expressions.
# SWITCH(<expression>, <value>, <result>[, <value>, <result>]…[, <else>])
# Parameters
# expression
# Any DAX expression that returns a single scalar value, where the expression is to be evaluated multiple times (for each row/context).
# value
# A constant value to be matched with the results of expression.
# result
# Any scalar expression to be evaluated if the results of expression match the corresponding value.
# else
# Any scalar expression to be evaluated if the result of expression doesn't match any of the value arguments.
# Return Value
# A scalar value coming from one of the result expressions, if there was a match with value, or from the else expression,
# if there was no match with any value.


def switch(match, *case_res_pairs):
    for pair in args[1:]:
        (case, res) = tuple(pair.split(','))
        if (str(match) == case):
            return res

#permutation - nPr


def permut(n, r):
    n = __builtins__['int'](n)
    r = __builtins__['int'](r)
    # eliminates the multiplication required for (n-r)! as the same term is
    # divided
    return str(reduce(__builtins__['int'].__mul__,
                      list(range(n - r + 1, n + 1)), 1))
    return str(reduce(__builtins__['int'].__mul__,
                      list(range(n - r + 1, n + 1)), 1))


# direct combination - nCr
def combin(n, r):
    n = __builtins__['int'](n)
    r = __builtins__['int'](r)
    r = min(r, n - r)  # to get least number of multiplications in reduce
    # eliminates the need to calculate 3 factorials by removing common terms
    # in n! and (n - r)!
    return str(reduce(__builtins__['int'].__mul__, list(
        range(n - r + 1, n + 1)), 1) / math.factorial(r))

# mathematical definition of combination to check result
# def comb_checker(n_, r_):
#    n = int(n_)
#    r = int(r_)
#    return str(math.factorial(n) / (math.factorial(r) * math.factorial(n - r)))
# combination with repetition - (n + r - 1)C(n - 1)


def combinA(n, r):
    n = __builtins__['int'](n)
    r = __builtins__['int'](r)
    new_n = n + r - 1
    new_r = n - 1
    # since mathematical formula of combA(n, r) is (n + r - 1)C(n - 1)
    return combin(new_n, new_r)

# factorial


def fact(n):
    n = __builtins__['int'](n)
    return str(math.factorial(__builtins__['int'](n)))

# Returns -1, 0, or 1 if a < 0, a == 0 or a > 0 respectively


def sign(x):
    x = __builtins__['int'](x)
    # using True = 1 and False = 0, the following expression returns the desired
    # result
    return str((x > 0) - (x < 0))