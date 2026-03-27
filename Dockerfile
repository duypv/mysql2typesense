FROM node:20-alpine AS base
WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
RUN npm install

COPY config ./config
COPY src ./src

CMD ["npm", "run", "sync:bootstrap"]