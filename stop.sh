#!/bin/bash
sudo kill `ps aux | grep aws_api | grep -v "grep" | head -3 | awk '{print $2}'`