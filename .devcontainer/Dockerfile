FROM mcr.microsoft.com/devcontainers/python:3.11

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /workspaces/research-mcp

ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/workspaces/research-mcp/.venv
