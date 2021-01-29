def get(url):
    import urllib.request
    response = urllib.request.urlopen(url)
    return response.read().decode('ascii')
