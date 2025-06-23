import type { NextConfig } from "next";
import { join } from 'path';
const nextConfig: NextConfig = {
output: 'standalone', // Required for Docker deployment
  env: {
    NEXT_PUBLIC_BACKEND_API_URL: process.env.NEXT_PUBLIC_BACKEND_API_URL || 'http://localhost:5000',
  },
};

export default nextConfig;
