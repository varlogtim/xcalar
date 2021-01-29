# Copyright 2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json

license_code = "H4sIAFbC61oAA22OyXaCMABF93yFH1AlTCoLFpZBBitaq1A3PYEEDwoJJlGBr68d7KrLN537AoEZFCUlljoC0qLMMeEYvVErzWEF2dMgILm0YhRdcuHBuqy632TgQAEHqwqKgrL6Ufk/3GHGvxDKSL9DHra1CJbb9CMd65LbNuXPjfsQWypQp8NZw4YakJYUYZteiLAUIG05Zn8iJt+zzkogI1KIstwlMKswsgpYcSzx8kCguDBsGZsGa7turdlGUNQctV4Zm87aGMtuecQKUyikz+ZhU7gILc9xtFdvvve6joGuOugY2sA8J7eWF94iUhNZX7U9mtTz6oi1syn7kzAS4J3NTl3Wz7OT3G4SYvdAu3LWL3VjX/TmBE+vt/gFEacN00PYajbxSQQaL6n7Yj+bh7Jg3NR9m0qfhiqafpEBAAA="

license_code2 = "H4sIAAAAAAAAA22OyXaCMABF93yF+1YLSKssWDAKiBRR1LrpCSTWVEhCCIN/Xzu46/JN575AIA4EpsRQJ7IU4QKRBsEtNQ4FKAF/HAWkkBJOYVsID1S4vP4lIwcIMEpKIE6UV/fK/+EO8eYboUy0G+RuG1EQZ4f3w4smuQPDvzduQ2Sosjofm4yPp7IUU4hs2hJhKLL6LGUN4ncpS6/kZ3k19oATKYR54RKQlwgagrdIavAHAaLlyNALsVwhk9nHM7apnvbrc73q9BCh2duxPZbJ4GtPpjy4U6/uHKrBU6U4ijazYekVRDBrEyvJcpUTD+2UB9RfTNtleb0OPbjFl21PqgDvFStyQ2HFrtr7rb/2mZDpYtqmGasTPez0xYAzkeaxmaXFJtj0XiTPmW/lnd5r7NO2ehg4zuULQvloNpIBAAA="

# The following license expires on Jan 7, 2021.  Prior to, and close to, that
# date this needs to be updated.

license_code_zero_nodes = "H4sIAPLYM1wAA22OwXKCMBiE73kKH6BqAAfpIQdFFNARiqjopfMbgkUgqSGI9ulrbb31trP77c4ucsp4zdJYkIRCCfKl43GKPMUkqFxwovcwCktQmZAVWXjLdfKemAO0YbL+ibXe4A746YE6HA4lS0kGZc1QKEXaUPU32pmAgs5zBgXcuX7m8ka2IPkTnUKVl7f/Cw/88efuM6JjXev6wLvYREuRMls0XBGM1jWTv1rDqM6PHFQjGSl2l9QYTwqQo1O0PEtpt9ziRyeSoWZ488bEURBk4IjIkz5XiXXVuK24u8gyK/uCVxsPM4+ZYWi5+VjTL2fLpUZL9/28rUBNJyuqt0L4fpne2ri1jv3InG8CQ08+VlP7mo63oYVjOiv2+mjw1syxu67i08xxZTasV267a7dlOIuHYFwK9A0+qvkmkAEAAA=="


def test_basic(client):
    client.create_license(license_code)
    lic = client.get_license()
    jsdata = json.loads(lic)
    assert (jsdata['nodeCount'] == '10')
    assert (jsdata['userCount'] == '10')
    assert (jsdata['product'] == 'Xcalar Data Platform')
    assert (jsdata['productFamily'] == 'Xcalar Data Platform')
    assert client.validate_license() is True

    # Update to a license that allows zero nodes in the cluster.
    client.update_license(license_code_zero_nodes)
    lic = client.get_license()
    jsdata = json.loads(lic)
    assert (jsdata['nodeCount'] == '0')
    assert client.validate_license() is False

    client.update_license(license_code2)
    lic = client.get_license()
    jsdata = json.loads(lic)
    assert (jsdata['nodeCount'] == '1025')
    assert (jsdata['userCount'] == '100')
    assert (jsdata['product'] == 'Xcalar Data Platform')
    assert (jsdata['productFamily'] == 'Xcalar Data Platform')
    assert client.validate_license() is True

    client.delete_license()
