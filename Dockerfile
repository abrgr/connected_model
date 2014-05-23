# DOCKER-VERSION 0.8.0

FROM docker.ezbds.com:5000/ezbds-base-with-node:0.1.0

#########################################
# ezbds install
#########################################

# install build tools
RUN yum install -y mysql-devel git cmake mysql-libs

# Install requirejs and nodeunit
RUN npm install -g requirejs nodeunit

ADD . /opt/connected_model

RUN cd /opt/connected_model \
    && npm install \
    && nodeunit tests/connected-model-tests.js
