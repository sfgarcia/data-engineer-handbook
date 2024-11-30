#!/bin/bash

# Fetch the latest changes from the original repository
git fetch upstream

# Merge the changes into your fork's branch
git checkout main
git merge upstream/main

# Push the changes to your fork on GitHub
git push origin main