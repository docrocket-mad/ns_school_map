#!/usr/bin/env python3
"""
Simple CORS proxy for Claude API
Run this script and use http://localhost:8080/api/anthropic as your API endpoint
"""
import json
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import sys

class CORSHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-api-key, anthropic-version')
        self.end_headers()

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_error(404, 'Not found')

    def do_POST(self):
        if self.path == '/api/anthropic':
            try:
                # Read the request body
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                
                # Get API key from headers
                api_key = self.headers.get('x-api-key')
                if not api_key:
                    self.send_error(401, 'Missing API key')
                    return
                
                # Forward request to Claude API
                headers = {
                    'x-api-key': api_key,
                    'anthropic-version': '2023-06-01',
                    'content-type': 'application/json',
                }
                
                response = requests.post(
                    'https://api.anthropic.com/v1/messages',
                    headers=headers,
                    data=post_data
                )
                
                # Send response back with CORS headers
                self.send_response(response.status_code)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                
                self.wfile.write(response.content)
                
            except Exception as e:
                print(f"Error: {e}")
                self.send_error(500, str(e))
        else:
            self.send_error(404, 'Not found')

if __name__ == '__main__':
    print("Starting CORS proxy for Claude API on http://localhost:8080")
    print("Use http://localhost:8080/api/anthropic as your API endpoint")
    print("Press Ctrl+C to stop")
    
    server = HTTPServer(('localhost', 8080), CORSHTTPRequestHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down proxy server...")
        server.shutdown()