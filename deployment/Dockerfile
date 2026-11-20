FROM python:3.14-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl --fail --location --retry 3 \
        https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
        --output /tmp/google-chrome.deb \
    && apt-get install -y --no-install-recommends /tmp/google-chrome.deb xvfb fonts-liberation \
    && rm -f /tmp/google-chrome.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.7
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable --no-cache

RUN groupadd --gid 10001 crawler \
    && useradd --uid 10001 --gid crawler --create-home crawler \
    && install -d -o crawler -g crawler /data/state /data/profiles \
    && install -d -m 1777 /tmp/.X11-unix
COPY --chmod=755 deployment/start-crawler.sh /usr/local/bin/start-crawler
ENV PATH="/app/.venv/bin:$PATH" \
    HOME=/home/crawler \
    PYTHONUNBUFFERED=1 \
    VC_HOST=0.0.0.0 \
    VC_DATA_DIR=/data/state \
    VC_PROFILE_ROOT=/data/profiles
USER crawler
EXPOSE 8091
ENTRYPOINT ["/usr/local/bin/start-crawler"]
CMD ["video-crawler"]
