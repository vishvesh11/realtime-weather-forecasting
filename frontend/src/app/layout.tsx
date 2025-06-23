// frontend/src/app/layout.tsx
import './globals.css';
import { Inter } from 'next/font/google'; // Import Inter font from Next.js

const inter = Inter({ subsets: ['latin'] });

export const metadata = {
  title: 'Realtime Weather Dashboard',
  description: 'Weather forecasting dashboard with ML models',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={inter.className}>{children}</body>
    </html>
  );
}