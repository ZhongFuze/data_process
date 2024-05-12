# Use the official Node.js 16 image.
# https://hub.docker.com/_/node
FROM node:16

LABEL maintainer="Zella Zhong <zella@mask.io>"

WORKDIR /nodejs_app

COPY package*.json ./

RUN npm install --only=production

COPY . .

ENTRYPOINT [ "node", "index.js" ]