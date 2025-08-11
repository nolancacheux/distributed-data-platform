#!/bin/bash

# === CONFIGURATION ===
IMAGE_NAME="m1-test-app"
DOCKER_USER="bafbi"
TAG="latest"
FULL_IMAGE="${DOCKER_USER}/${IMAGE_NAME}:${TAG}"

# === BUILD ===
echo "🔨 Building Docker image..."
docker build -t $FULL_IMAGE .

if [ $? -ne 0 ]; then
  echo "❌ Build failed"
  exit 1
fi

# === LOGIN (only if not already logged in) ===
if ! docker info | grep -q "Username: $DOCKER_USER"; then
  echo "🔐 Logging into Docker Hub as $DOCKER_USER..."
  docker login -u $DOCKER_USER
  if [ $? -ne 0 ]; then
    echo "❌ Login failed"
    exit 1
  fi
fi

# === PUSH ===
echo "📤 Pushing image to Docker Hub..."
docker push $FULL_IMAGE

if [ $? -eq 0 ]; then
  echo "✅ Successfully pushed: $FULL_IMAGE"
else
  echo "❌ Push failed"
  exit 1
fi
