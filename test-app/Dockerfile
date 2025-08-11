FROM node:20-alpine

# Enable Corepack and prepare pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy only package manager files first to leverage Docker cache
COPY package.json pnpm-lock.yaml ./

# Set non-interactive mode to avoid prompts
ENV CI=true

# Install deps (cached if package files unchanged)
RUN pnpm install --frozen-lockfile

# Copy the rest of the source code (frequent changes = cache busting only here)
COPY src ./src

CMD ["node", "src/index.js"]
