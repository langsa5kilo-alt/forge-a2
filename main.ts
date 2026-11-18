// SPDX-License-Identifier: MIT
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const arch = process.arch === 'x64' ? 'amd64' : process.arch;
const child = spawn(fileURLToPath(new URL(`./forge-${arch}`, import.meta.url)), [], {
  env: process.env,
  stdio: 'inherit',
});
child.on('error', (error) => { console.error(error); process.exit(1); });
child.on('exit', (code, signal) => process.exit(signal ? 1 : (code ?? 1)));
