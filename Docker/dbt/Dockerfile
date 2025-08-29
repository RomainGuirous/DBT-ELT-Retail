FROM python:3.11-slim-bullseye

LABEL maintainer="dbt-elt-retail"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Installer dépendances système nécessaires pour compiler des paquets Python natifs
RUN rm -f /etc/apt/sources.list.d/* || true \
    && printf '%s\n' \
       'deb https://deb.debian.org/debian bullseye main contrib non-free' \
       'deb https://deb.debian.org/debian bullseye-updates main contrib non-free' \
       'deb https://security.debian.org/debian-security bullseye-security main contrib non-free' \
      > /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       git \
       libpq-dev \
       ca-certificates \
       curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Installer pip et dbt-postgres (version contrôlée)
RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir dbt-postgres==1.5.0

# Montrer la version pour debug et validation rapide
CMD ["dbt", "debug"]
