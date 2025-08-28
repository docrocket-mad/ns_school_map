# AI Assistant Setup Guide

## CORS Issue Fix

The AI Assistant uses Claude API, which requires CORS handling for browser security. Here are three solutions:

### Option 1: Use Local Proxy (Recommended)

1. **Install Python** (if not already installed)
2. **Install requests library:**
   ```bash
   pip install requests
   ```
3. **Run the proxy server:**
   ```bash
   python cors_proxy.py
   ```
4. **Keep the terminal open** - the proxy runs at http://localhost:8080
5. **Refresh your browser** - the app will auto-detect the proxy

### Option 2: Browser Extension (Quick Fix)

1. **Install CORS extension** like "CORS Unblock" (Chrome) or "CORS Everywhere" (Firefox)
2. **Enable the extension** for your domain
3. **Refresh the page** and try the AI features

### Option 3: Use Fallback Templates

The system provides fallback templates when API calls fail:
- Email templates for schools
- Basic pattern analysis
- CRM recommendations

## Testing AI Features

Once CORS is resolved:

1. **Configure API Key:**
   - Click "AI Tools" tab
   - Click "⚙️ Configure Claude API"
   - Enter your Claude API key from https://console.anthropic.com

2. **Test Commands:**
   - `"write email for [school name]"` → AI-generated email
   - `"analyze patterns"` → Conversion insights
   - `"help"` → Available commands
   - `"pipeline analysis"` → Sales funnel review

## Troubleshooting

- **NetworkError**: CORS issue - use proxy or browser extension
- **401 Error**: Invalid API key - reconfigure in AI Tools
- **No response**: Check if proxy is running on localhost:8080

## API Key Security

- Stored locally in browser only
- Not transmitted to any other servers
- Can be cleared anytime via "Configure API" button