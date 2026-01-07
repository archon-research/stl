# Use Node.js 20 LTS
FROM node:20-slim AS base

# Install pnpm globally and prepare it
ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml* package-lock.json* ./

# Install dependencies
FROM base AS deps
RUN pnpm install --frozen-lockfile --prod=false

# Build stage
FROM base AS builder
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Generate Ponder types and schema
RUN pnpm codegen

# Production stage
FROM base AS runner
ENV NODE_ENV=production

# Copy dependencies and built files
COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app ./

# Create non-root user for security with home directory
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 --home /home/ponder ponder && \
    chown -R ponder:nodejs /app && \
    chown -R ponder:nodejs /home/ponder

# Set environment for corepack to use user home
ENV COREPACK_HOME=/home/ponder/.cache/corepack
USER ponder

# Expose Ponder's default port
EXPOSE 42069

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD node -e "require('http').get('http://localhost:42069/health', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Start Ponder
CMD ["pnpm", "start"]

