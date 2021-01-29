# Hadoop 2.7.1 Docker image
This project is based on sequenceiq/hadoop-docker:2.7.1 and adds the qa sample dataset to /datasets/qa/.

You have several options to run this image:
- Use docker
- Use docker-compose
- Simply run 'make' (recommended)

To obtain docker-compose:

```
curl -L https://github.com/docker/compose/releases/download/1.8.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
```

## Build the Hadoop image
via docker

```
docker build -t hdfs-sanity .
```

or if you have docker-compose

```
export HOSTNAME=`hostname -s`
export DNSDOMAIN=`dnsdomainname`
docker-compose build .
```

or

```
make build
```

## Run Hadoop
Via docker

```
docker run -d -p 2122:2122 -p 8020:9000 -p 8088:8088 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 --hostname hdfs-sanity.local hdfs-sanity
```

or via docker-compose

```
docker-compose up -d
```

or

```
make run
```

You should now be able to access the Hadoop Admin UI at

http://<host>:8088/cluster

And the DFS Admin UI at

http://<host>:50070


