// PM2 ecosystem config for api_data_pipeline
// Place this file at: /var/www/kaviarasan/data_pipeline/ecosystem.config.cjs
//
// Schedule: daily at 20:43 IST (15:13 UTC)  →  cron_restart: '13 15 * * *'
//
// The pipeline is a one-shot batch job:
//   - autorestart: false  → PM2 will NOT re-launch it on normal exit
//   - cron_restart        → PM2 re-launches it at the cron time each day
//   - max_restarts: 0     → no automatic retries on crash (the pipeline
//                           handles its own resume-from-watermark on the
//                           next scheduled run)

'use strict';

module.exports = {
  apps: [
    {
      // ── Identity ──────────────────────────────────────────────────────────
      name: 'data_pipeline',

      // ── Entry point ───────────────────────────────────────────────────────
      script: '/bin/bash',
      args: 'run_pipeline.sh',
      cwd: '/var/www/kaviarasan/data_pipeline',

      // ── Scheduling ────────────────────────────────────────────────────────
      // Cron fires at 20:43 IST every day (15:13 UTC).
      // Change this to any standard 5-field cron expression.
      cron_restart: '13 15 * * *',

      // ── Restart policy ────────────────────────────────────────────────────
      // Batch job — do NOT loop-restart on normal exit.
      autorestart: false,
      max_restarts: 0,

      // ── Execution environment ─────────────────────────────────────────────
      watch: false,
      env: {
        PYTHONUNBUFFERED: '1',
      },

      // ── Logging ───────────────────────────────────────────────────────────
      out_file:        '/var/www/kaviarasan/data_pipeline/logs/pipeline_out.log',
      error_file:      '/var/www/kaviarasan/data_pipeline/logs/pipeline_err.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs:      false,

      // ── Resource guard (optional) ─────────────────────────────────────────
      // PM2 will restart the process if it ever exceeds 3 GB RSS.
      max_memory_restart: '3G',
    },
  ],
};
