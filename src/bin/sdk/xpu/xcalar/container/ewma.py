def ewma(csvCol, winSize, rounding=3):
    if winSize == 0:
        return csvCol
    row = csvCol.split(',')
    q = [float(numStr) for numStr in row]
    winSize = min(winSize, len(q))
    result = ''
    offset = 0
    while offset < len(q):
        p = []
        for i in range(offset, min(offset + winSize, len(q))):
            p.append(q[i])
        alpha = 2.0 / (winSize + 1)
        oneAlpha = 1.0 - alpha
        c = []
        for i in range(0, len(p) - 2):
            c.append(pow(oneAlpha, 2 + i))
        if len(p) > 0:
            numerator = p[0]
        if len(p) > 1:
            numerator = numerator + oneAlpha * p[1]
        for i in range(0, len(c)):
            numerator = numerator + c[0] * p[2 + i]
        denom = 1 + oneAlpha
        for i in range(0, len(c)):
            denom = denom + c[i]
        myF = numerator / denom
        result = result + str(round(myF, rounding)) + ','
        offset = offset + 1
    return result[:-1]
