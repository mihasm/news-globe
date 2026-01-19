# Dev Scripts

This folder contains debugging and development utilities for the News Globe project.

## Frontend Debug Script

The `frontend_debug.py` script uses Playwright to launch Firefox in headless mode and capture fresh console logs from the frontend application.

### Prerequisites

1. Install dependencies:
   ```bash
   pip3 install -r requirements.txt
   ```

2. Make sure the frontend services are running:
   ```bash
   docker-compose up -d nginx api
   ```

### Usage

Run the debugging script:

```bash
python3 frontend_debug.py
```

### What it does

1. Launches Firefox in headless mode
2. Navigates to `http://localhost` (the frontend)
3. Captures all console logs (errors, warnings, info, etc.)
4. Prints logs in real-time with timestamps and locations
5. Waits 10 seconds to collect logs
6. Provides a summary of log levels

### Output Format

```
[HH:MM:SS] LEVEL: log message
    Location: file.js:line_number
```

This format makes it easy for AI coding agents to grep and analyze the logs for debugging purposes.

### Troubleshooting

- If the script fails to connect, ensure nginx and api services are running
- If Playwright browsers aren't installed, run: `playwright install firefox`
- The script assumes the frontend is available at `http://localhost` (port 80)