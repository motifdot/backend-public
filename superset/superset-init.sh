#!/bin/bash

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 

# Starting server
/bin/sh -c /usr/bin/run-server.sh
