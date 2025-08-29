# Gmail Integration Setup Guide

## Overview
The Gmail integration allows you to send emails directly from the CRM with personalized templates and automatic status tracking.

## Prerequisites
1. Google Cloud Project with Gmail API enabled
2. OAuth 2.0 credentials configured

## 🚨 IMPORTANT: OAuth Redirect Issue Fix

**The Problem:** File:// protocol doesn't work with Google OAuth
**The Solution:** Use the local web server instead

### Quick Fix - Use Local Web Server
1. **Run the local server:**
   ```bash
   python serve.py
   ```
2. **Open in browser:** http://localhost:8000 (instead of opening the HTML file directly)
3. **Configure OAuth origins:** Add `http://localhost:8000` to your Google Cloud Console

## Setup Steps

### 1. Create Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the Gmail API:
   - Go to APIs & Services > Library
   - Search for "Gmail API"
   - Click Enable

### 2. Create OAuth 2.0 Credentials
1. Go to APIs & Services > Credentials
2. Click "Create Credentials" > "OAuth client ID"
3. Select "Web application"
4. Add authorized JavaScript origins:
   - `http://localhost:8000` (for local web server)
   - `http://127.0.0.1:8000` (alternative localhost)
5. Add authorized redirect URIs:
   - `http://localhost:8000` (for local web server)
   - `http://127.0.0.1:8000` (alternative localhost)
6. Save and note down:
   - **Client ID** (looks like: `123456789-abc123.apps.googleusercontent.com`)
   - **API Key** (from API Keys section)

### 3. Configure in CRM
1. Open your CRM application
2. Go to **AI Tools** tab
3. Click **"⚙️ Configure Gmail API"**
4. Enter your **Google API Key**
5. Enter your **Google Client ID**
6. Click **"⚙️ Configure Gmail API"** again to authorize
7. Sign in with your Gmail account
8. Grant permissions for sending emails

### 4. Set User Profile
1. Click **"👤 Set User Profile"**
2. Enter your name (appears in email signatures)
3. Enter your phone number (appears in email signatures)
4. Enter your email address (appears in email signatures)

## How to Use

### Quick Email Templates
Click on any school marker to open details, then:
- **📤 Send Introduction**: For schools never contacted
- **🔄 Send Follow-up**: For schools that received first outreach
- **💬 Continue Conversation**: For schools that replied
- **✏️ Custom Email**: Write your own email

### Email Templates Include:
- **Introduction**: Professional introduction to Mad Science programs
- **Follow-up**: Gentle follow-up after initial outreach
- **Reply Template**: Response to schools that showed interest

### Automatic Features
- **Status Tracking**: Updates school status after sending emails
- **Contact Logging**: Adds email timestamp to notes
- **Smart Personalization**: Customizes content based on school data

## Troubleshooting

### Common Issues:
1. **"Gmail setup failed"**: Check API key and Client ID
2. **"Authorization failed"**: Ensure redirect URIs are configured correctly
3. **"Email send failed"**: Check Gmail API quotas and permissions

### API Limits:
- Gmail API has daily quotas
- Production apps need verification for higher limits
- For development, limits are sufficient for testing

### Security Notes:
- API keys are stored locally in browser
- OAuth tokens are managed by Google
- No email data is stored on external servers

## Production Deployment

For production use:
1. Update authorized origins to your domain
2. Consider OAuth app verification for higher quotas
3. Test thoroughly with different email scenarios
4. Monitor API usage in Google Cloud Console

## Support
- Google Cloud Console for API configuration
- Gmail API documentation for detailed guides
- Test with a small number of schools first