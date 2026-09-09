FROM node:24-bookworm-slim AS dependencies
WORKDIR /app
ENV NUXT_TELEMETRY_DISABLED=1
COPY package.json package-lock.json ./
RUN npm ci

FROM dependencies AS build
COPY . .
RUN npm run typecheck && npm run build

FROM node:24-bookworm-slim AS lms
WORKDIR /app
ENV NODE_ENV=production HOST=0.0.0.0 PORT=3000
COPY --from=build --chown=node:node /app/.output ./.output
USER node
CMD ["node", ".output/server/index.mjs"]

FROM dependencies AS certificate
COPY certificate ./certificate
USER node
ENV NODE_ENV=production
CMD ["node", "certificate/server.ts"]
