from datetime import datetime
import time
from dateutil.relativedelta import relativedelta


def add_business_months(start_date, months_to_add):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    date_after_month = start_date + relativedelta(months=months_to_add)
    return date_after_month


def dategeinputdate(o_orderdate, lv_inputdate):
    orderdate = o_orderdate
    inputdate = lv_inputdate
    neworderdate = time.strptime(orderdate, "%Y-%m-%d")
    newinputdate = time.strptime(inputdate, "%Y-%m-%d")
    if (neworderdate) >= (newinputdate):
        return True
    else:
        return False

def dateltinputdate(datecol1, datecol2, month_interval):
    lv_datecol1 = datecol1
    lv_datecol2 = datecol2
    lv_month_interval = month_interval
    newdatecol1 = datetime.strptime(lv_datecol1, "%Y-%m-%d")
    newdatecol2 = datetime.strptime(lv_datecol2, "%Y-%m-%d")
    finalnewinputdate = newdatecol2 + relativedelta(months=lv_month_interval)
    if (newdatecol1) < (finalnewinputdate):
        return True
    else:
        return False

def dateleshipdate(datecol1, datecol2, day_interval):
    lv_datecol1 = datecol1
    lv_datecol2 = datecol2
    lv_day_interval = day_interval
    newdatecol1 = datetime.strptime(lv_datecol1, "%Y-%m-%d")
    newdatecol2 = datetime.strptime(lv_datecol2, "%Y-%m-%d")
    finalnewinputdate = newdatecol2 - relativedelta(days=lv_day_interval)
    if (newdatecol1) <= (finalnewinputdate):
        return True
    else:
        return False

    
def dateltyearinputdate(datecol1, datecol2, year_interval):
    lv_datecol1 = datecol1
    lv_datecol2 = datecol2
    lv_year_interval = year_interval
    newdatecol1 = datetime.strptime(lv_datecol1, "%Y-%m-%d")
    newdatecol2 = datetime.strptime(lv_datecol2, "%Y-%m-%d")
    finalnewinputdate = newdatecol2 + relativedelta(years=lv_year_interval)
    if (newdatecol1) < (finalnewinputdate):
        return True
    else:
        return False

if __name__ == '__main__':
    #add_business_months("2007-01-01", 3)
    finaldate = dategeinputdate("2008-01-01", "2008-01-01")
    print finaldate