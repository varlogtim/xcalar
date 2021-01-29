## Package lists by OS


See [EL6 Wiki](http://wiki.int.xcalar.com/mediawiki/index.php/Enterprise_Linux)

Obtained via:

    rpm -qa | sed -Ee 's/-[0-9][0-9a-zA-Z\._-]*$//g'| sort

### Minimal

Packages listed as present in a 'minimal' install of the specified OS
in VMware

    el6 => CentOS-6.8-x86_64-minimal.iso
    el7 => CentOS-7-x86_64-Minimal-1511.iso

### Docker

Packages listed are present in the base docker image of the specified
OS

    el6 => centos:6
    el7 => centos:7


## View diff

    $ vimdiff docker/el6.txt minimal/el6.txt
    $ vimdiff docker/el7.txt minimal/el7.txt
