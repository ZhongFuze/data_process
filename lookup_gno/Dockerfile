# Use the official Node.js 16 image.
# https://hub.docker.com/_/node
FROM node:16

LABEL maintainer="Zella Zhong <zella@mask.io>"

RUN apt-get update -y && \
    apt-get upgrade -y --force-yes && \
    apt-get install -y --force-yes supervisor

WORKDIR /nodejs_app

COPY package*.json ./

RUN npm install --only=production

COPY . .

EXPOSE 22222
ENTRYPOINT ["./nodejs_app/run.sh"]