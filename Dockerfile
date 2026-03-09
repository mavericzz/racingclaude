FROM node:22-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# Build timestamp: 2026-03-09T22:00:00
RUN echo "deployed-v2-ai-analysis" > /app/.build-marker

ENV NODE_ENV=production
EXPOSE 3004

CMD ["node", "--import", "tsx", "src/server/index.ts"]
