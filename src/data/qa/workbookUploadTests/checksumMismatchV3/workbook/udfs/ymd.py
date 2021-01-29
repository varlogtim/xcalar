def ymd(year, month, day):
    if int(month) < 10:
        month = "0" + str(month)
    if int(day) < 10:
        day = "0" + str(day)
    return str(year) + str(month) + str(day)