FROM node:5
MAINTAINER Octoblu, Inc. <docker@octoblu.com>

ENV NPM_CONFIG_LOGLEVEL error
RUN npm install --silent --global yarn

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json /usr/src/app/
RUN yarn install
COPY . /usr/src/app/

CMD [ "node", "command.js" ]
