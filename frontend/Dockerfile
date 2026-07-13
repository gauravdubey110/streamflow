# ─────────────────────────────────────────────────────────────────────────────
# frontend — multi-stage Dockerfile
# SPEC-21 R3: node:20-alpine build stage; nginx:alpine runtime with custom conf.
#
# Build from repo root:
#   docker build -f frontend/Dockerfile -t streamflow-frontend:latest .
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package manifests first (cache npm install layer)
COPY frontend/package.json frontend/package-lock.json* ./

# Install all dependencies (including devDependencies needed for build)
RUN npm ci --prefer-offline

# Copy source code
COPY frontend/ ./

# Build the Vite SPA — produces /app/dist
RUN npm run build

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM nginx:alpine AS runtime

# Remove default Nginx config and install our custom one
RUN rm /etc/nginx/conf.d/default.conf
COPY frontend/nginx.conf /etc/nginx/conf.d/streamflow.conf

# Copy the Vite build output
COPY --from=builder /app/dist /usr/share/nginx/html

# Nginx listens on port 80 by default; mapped to host 3000 in docker-compose.yml
EXPOSE 80

# nginx:alpine default CMD is "nginx -g 'daemon off;'" — no override needed
