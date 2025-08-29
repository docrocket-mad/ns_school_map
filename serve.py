#!/usr/bin/env python3
"""
Simple HTTP server to serve the Mad Science CRM locally.
This solves OAuth redirect issues with file:// protocol.

Usage: python serve.py
Then open: http://localhost:8000
"""

import http.server
import socketserver
import webbrowser
import os
from pathlib import Path

PORT = 8000
DIRECTORY = Path(__file__).parent / "docs"

class CORSHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DIRECTORY), **kwargs)
    
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        super().end_headers()

def main():
    print("Starting Mad Science CRM Local Server...")
    print(f"Serving from: {DIRECTORY}")
    print(f"Server running at: http://localhost:{PORT}")
    print("This solves OAuth redirect issues for Gmail integration")
    print("\nFor Gmail setup:")
    print("1. In Google Cloud Console, add http://localhost:8000 as authorized origin")
    print("2. Use http://localhost:8000 instead of file:// URL")
    print("\nPress Ctrl+C to stop the server")
    
    # Change to docs directory
    os.chdir(DIRECTORY)
    
    with socketserver.TCPServer(("", PORT), CORSHTTPRequestHandler) as httpd:
        try:
            # Open browser automatically
            webbrowser.open(f'http://localhost:{PORT}')
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped by user")
            httpd.shutdown()

if __name__ == "__main__":
    main()