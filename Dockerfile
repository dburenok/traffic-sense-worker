FROM node:18-alpine
LABEL maintainer="dmitriyburenok@gmail.com"
COPY . .
RUN npm install
CMD node worker.js
