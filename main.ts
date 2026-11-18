// SPDX-License-Identifier: MIT
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// The forge runs as this launcher's only child, with this process's stdio. Signals the trial sends
// (SIGTERM on a stop) are forwarded to it, and when it exits so does the launcher: one lifecycle.
const arch = process.arch === 'x64' ? 'amd64' : process.arch;
const child = spawn(fileURLToPath(new URL(`./forge-${arch}`, import.meta.url)), [], {
  env: process.env,
  stdio: 'inherit',
});
for (const signal of ['SIGTERM', 'SIGINT', 'SIGHUP']) {
  process.on(signal, () => { try { child.kill(signal); } catch {} });
}
child.on('error', (error) => { console.error(error); process.exit(1); });
child.on('exit', (code, signal) => process.exit(signal ? 1 : (code ?? 1)));
