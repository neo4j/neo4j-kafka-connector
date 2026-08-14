#!/bin/bash -eu

# This file exists to prevent issues with creating a script dynamically, which can occur is the build agent is running low on disk.
dip compose down --rmi local