FROM node:24-bookworm-slim AS sdk
WORKDIR /repo/sdk/typescript
COPY sdk/typescript/package.json sdk/typescript/package-lock.json ./
RUN npm ci
COPY sdk/typescript/ ./
RUN npm test

FROM node:24-bookworm-slim AS dependencies
WORKDIR /repo/examples/nuxt-lms
ENV NUXT_TELEMETRY_DISABLED=1
COPY --from=sdk /repo/sdk/typescript/package.json /repo/sdk/typescript/package.json
COPY --from=sdk /repo/sdk/typescript/dist /repo/sdk/typescript/dist
COPY examples/nuxt-lms/package.json examples/nuxt-lms/package-lock.json ./
RUN npm ci

FROM dependencies AS build
COPY examples/nuxt-lms/ .
RUN npm run typecheck && npm run build

FROM node:24-bookworm-slim AS lms
WORKDIR /app
ENV NODE_ENV=production HOST=0.0.0.0 PORT=3000
COPY --from=build --chown=node:node /repo/examples/nuxt-lms/.output ./.output
USER node
CMD ["node", ".output/server/index.mjs"]

FROM dependencies AS certificate
COPY examples/nuxt-lms/certificate ./certificate
USER node
ENV NODE_ENV=production
CMD ["node", "certificate/server.ts"]
