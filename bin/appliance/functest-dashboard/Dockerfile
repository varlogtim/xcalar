FROM centos:7

# Install the EPEL repository
RUN yum -y install \
  epel-release

RUN yum -y install \
  wget

# To get pre-built xccli
RUN wget http://repo.xcalar.net/builds/prod/xcalar-1.0.3.17-625.e60218ef-installer
RUN bash ./xcalar-1.0.3.17-625.e60218ef-installer --noStart && rm -f xcalar-1.0.3.17-625.e60218ef-installer

RUN mkdir -p /usr/src/functest
WORKDIR /usr/src/functest
COPY package.json /usr/src/functest/

RUN npm install

COPY . /usr/src/functest
EXPOSE 3001
CMD [ "npm", "start" ]
