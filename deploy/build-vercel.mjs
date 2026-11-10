import { copyFile, mkdir } from 'node:fs/promises';

await mkdir('dist/static', { recursive: true });
await Promise.all([
  copyFile('static/index.html', 'dist/index.html'),
  copyFile('static/app.js', 'dist/static/app.js'),
  copyFile('static/style.css', 'dist/static/style.css')
]);
