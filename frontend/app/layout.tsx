import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import "../src/header.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "SEM Landing Page Catalogue",
  description: "SEM landing pages catalogue",
  icons: { icon: "/sem-catalogue/mi-logo.svg" },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <div style={{ minHeight: "100vh", background: "#ffffff" }}>
          <div style={{ padding: 0, background: "#ffffff" }}>
            <div className="header">
              <div className="header-background"></div>
              <div className="header-content">
                <img alt="Marketplace Innovate Logo" loading="lazy" width="32" height="32" decoding="async" className="header-logo" style={{ color: "transparent" }} src="/sem-catalogue/mi-logo.svg" />
                <div className="header-title"><span>Marketplace</span><span>Innovate</span></div>
              </div>
            </div>
          </div>
          <main style={{ background: "transparent" }}>
            {children}
          </main>
        </div>
      </body>
    </html>
  );
}
