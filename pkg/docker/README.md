## Xcalar in Docker

### Overview

The Dockerfiles and the `docker-cluster` tool let you build and run
Xcalar in a container, following best practices at this time. These
can be leverages on baremetal machines without incurring the virtualization
tax, they can be used to do local network or any other kind of testing.
The xcalar containers spin up in seconds, and can be quickly destroyed.

### Features

- Proper init program is used for PID1, so zombies are reaped and signals
are forwarded.
- Networking is isolated, you can run the same image on the same host
- No shared host directories for XcalarRootCompletePath. We use a docker
persistent volume, so you don't end up with directories on your host with
permissions you can't change.
- Repeatable installation and upgrade experience. Just load the new image
and destroy/create. Settings/files are safe.
- User controls mapping directories from the host into the container
- The container has sshd running inside of it. Use this `docker-cluster ssh`
to test your automation on the containers. There's also `cssh` to run a
command on the whole cluster.
- Security, anyone who hacks into your jupyter, doesn't have access to the host!
- SSL certificates for int.xcalar.com domain names

### QuickStart

Load the image from internal registry

    $ docker-cluster run -i registry.int.xcalar.com/xcalar/xcalar:1.3.1-1742.el7 -n 3

Or load image directly from netstore

    $ docker-cluster load /netstore/builds/Release/xcalar-1.3.1/prod/xcalar-1.3.1-1742.el7.tar.gz
    $ docker-cluster run -n 3
    $ docker-cluster ssh xcalar-0

Now try browsing to you https://yourhost.int.xcalar.com . If port 443 was already taken
(by apache or otherwise), `docker-cluster run` should've printed out an alternate port to use.

### Watch the sample run on asciinema

https://asciinema.org/a/QWuzJqqPLUxbng7YQuYfG8AMC

### Build a new image given an installer

Use `INSTALLER_URL` pointing to a web server with your build. You can turn on
any file on netstore to a http URL by simply prefixing it with http://. When
referencing the installer, please use the link to the concrete/versioned installer
(the one with the version in it), not a symlink or virtual build like `xcalar-latest-installer`.
The reason for using the real URL, is that docker won't be able to distinguish
`xcalar-latest` from build to build and it will cache the wrong thing. If you want to
use one of those `latest` URLs, pass `DOCKER_BUILD_EXTRA_FLAGS=--no-cache`.

    $ make INSTALLER_URL=http://netstore.int.xcalar.com/builds/Release/xcalar-1.3.1/prod/xcalar-1.3.1-1742-installer
    $ make INSTALLER_URL=http://netstore.int.xcalar.com/builds/byJob/BuildTrunk/xcalar-latest-installer-prod \
            DOCKER_BUILD_EXTRA_FLAGS=--no-cache

    # start 3 node cluster
    $ docker-cluster -n 3

### Extend an existing existing image to customize (add data, etc)

Create a new Dockerfile, where you specify `FROM xcalar:latest` or similar tag.

    FROM xcalar:1.3.1-1742
    ...

### Wiki

See http://wiki.int.xcalar.com/mediawiki/index.php/Docker#QuickStart
