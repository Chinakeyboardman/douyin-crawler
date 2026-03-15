#!/usr/bin/env node

/**
 * Open browser with Douyin — for login or profile setup.
 * Uses the same profile as douyin-scraper.js so logins/settings are reused.
 *
 * Usage: node open-browser.js
 *
 * Environment Variables:
 * - BROWSER_USER_DATA_DIR: Chrome profile directory (default: ./browser-profile)
 */

const { chromium } = require('patchright');
const path = require('path');
const fs = require('fs');
const readline = require('readline');

const USER_DATA_DIR = process.env.BROWSER_USER_DATA_DIR || path.join(__dirname, 'browser-profile');
const DOUYIN_URL = 'https://www.douyin.com/?recommend=1';

async function main() {
  console.log('🌐 Opening browser (same profile as scraper)...');
  console.log(`   📂 Profile: ${USER_DATA_DIR}`);
  console.log('   🔗 Opening Douyin for login...');
  console.log('   Log in, then press Enter in this terminal to close.\n');

  if (!fs.existsSync(USER_DATA_DIR)) {
    fs.mkdirSync(USER_DATA_DIR, { recursive: true });
  }

  const context = await chromium.launchPersistentContext(USER_DATA_DIR, {
    channel: 'chrome',
    headless: false,
    viewport: null,
  });

  try {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  } catch (e) {
    // ignore
  }

  let page = context.pages()[0];
  if (!page) {
    page = await context.newPage();
  }
  await page.goto(DOUYIN_URL, { waitUntil: 'domcontentloaded' });

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  await new Promise((resolve) => {
    rl.question('Press Enter to close browser... ', () => {
      rl.close();
      resolve();
    });
  });

  await context.close();
  console.log('✅ Browser closed.');
}

main().catch((err) => {
  console.error('❌', err.message);
  process.exit(1);
});
