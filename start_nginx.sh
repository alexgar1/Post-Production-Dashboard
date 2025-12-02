#!/bin/bash

NGINX_CONF="/home/alex_g/src/nginx/nginx.conf"
NGINX_BIN="/usr/sbin/nginx"

# Check if nginx is installed
if ! command -v $NGINX_BIN &> /dev/null; then
    echo "nginx is not installed. Installing..."
    sudo apt-get update
    sudo apt-get install -y nginx
fi

# Start nginx with custom config
echo "Starting nginx with config: $NGINX_CONF"
sudo $NGINX_BIN -c $NGINX_CONF

echo "Nginx is running!"
echo "Visit http://localhost:8080 in your browser"
echo ""
echo "Useful commands:"
echo "  Stop nginx: sudo nginx -s stop"
echo "  Reload config: sudo nginx -s reload"
echo "  View error log: tail -f /tmp/nginx_error.log"
echo "  View access log: tail -f /tmp/nginx_access.log"
