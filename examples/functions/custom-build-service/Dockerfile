# A user-supplied Dockerfile for a `build:` service. Relay builds this image
# from the function's SELECTED source (the same .gitignore-driven selection that
# fingerprints a function), using the Docker Engine API — no Docker CLI and no
# privileged build daemon beyond the Engine Relay already talks to.
#
# The image's own ENTRYPOINT/CMD are preserved; Relay never overrides them for a
# build-source service.
FROM node:24-alpine

WORKDIR /app

# Copy the selected function source (template.yaml is excluded by Relay).
COPY . /app

# A custom build step: here, just a marker, but this is where a real project
# would run its own bundler/compiler.
RUN echo "custom build step" > /app/build-info.txt

EXPOSE 3000
ENTRYPOINT ["node", "/app/server.js"]
