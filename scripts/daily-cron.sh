#!/bin/bash
# Daily RacingClaude pipeline - run via cron
# Logs to ~/racingclaude/logs/daily-YYYY-MM-DD.log

set -euo pipefail

export PATH="/Users/rahulsharma/.nvm/versions/node/v22.22.0/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"

cd /Users/rahulsharma/racingclaude

# Create logs dir
mkdir -p logs

DATE=$(date +%Y-%m-%d)
LOG="logs/daily-${DATE}.log"

echo "=== Daily pipeline started at $(date) ===" >> "$LOG"
npx tsx src/scripts/dailyPipeline.ts >> "$LOG" 2>&1
echo "=== Finished at $(date) ===" >> "$LOG"
