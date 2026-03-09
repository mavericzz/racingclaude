FROM node:22-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

EXPOSE 3004

CMD ["node", "--import", "tsx", "src/server/index.ts"]
