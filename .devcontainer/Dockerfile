FROM python:3.11.9-slim

RUN apt-get update && apt-get install -y git gcc python3-dev procps pipx

# Generate locale necessary for pre-commit and git commit
RUN apt-get install -y --no-install-recommends locales; echo \"en_US.UTF-8 UTF-8\" >> /etc/locale.gen; locale-gen

# Create the user
RUN groupadd --gid 1000 vscode
RUN useradd --uid 1000 --gid 1000 -m vscode --shell /bin/bash

ENV PATH=$PATH:/home/vscode/.local/bin
ENV SENTRY_ENABLED=false

# Install poetry as the vscode ser
RUN su -l vscode -c "pipx ensurepath && pipx install poetry==1.8.3"

USER vscode
# Set VSCode as the core editor for git
RUN git config --global core.editor "code --wait"
RUN git config --global --add safe.directory /workspaces/toolkit