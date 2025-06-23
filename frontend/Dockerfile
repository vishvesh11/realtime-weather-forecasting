# frontend/Dockerfile
# Use the official Node.js image for the build stage
FROM node:20-alpine AS builder

# Set the working directory
WORKDIR /app


COPY package.json ./
COPY pnpm-lock.yaml ./

# Install dependencies
RUN npm install --frozen-lockfile # Or pnpm install --frozen-lockfile / yarn install --frozen-lockfile

# Copy the rest of the application code
COPY . .

# Build the Next.js application
RUN npm run build # Or pnpm build / yarn build

# Use a lean production image for the final stage
FROM node:20-alpine AS runner

WORKDIR /app

ENV NEXT_PUBLIC_API_BASE_URL=http://dashboard-backend-service:5000
ENV INFLUXDB_URL=http://localhost:8086
ENV INFLUXDB_TOKEN=my-super-secret-token
ENV INFLUXDB_ORG=my-organization
ENV INFLUXDB_BUCKET=weather_data
# Copy necessary files from the builder stage
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

EXPOSE 3000

CMD ["node", "server.js"]