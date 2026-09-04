# Multi-stage build for a Vite/React app.
# NOTE: this is a template - ./frontend doesn't have a package.json yet.
# Adjust the build command/output dir if your setup differs.

# ---- build stage ----
FROM node:20-slim AS build

WORKDIR /app

# Copy lockfiles first so this layer is cached across source changes.
COPY package.json package-lock.json* ./
RUN npm ci

COPY . .
RUN npm run build
# Vite's default build output is ./dist

# ---- serve stage ----
# Static build served by nginx, not Node - no JS runtime needed in production.
FROM nginx:1.27-alpine

COPY --from=build /app/dist /usr/share/nginx/html

# Minimal SPA config: serve index.html for unknown paths so client-side
# routing (react-router, etc.) works behind the reverse-proxy nginx.
RUN printf 'server {\n\
    listen 80;\n\
    root /usr/share/nginx/html;\n\
    index index.html;\n\
    location / {\n\
        try_files $uri $uri/ /index.html;\n\
    }\n\
}\n' > /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
