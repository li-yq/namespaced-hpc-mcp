FROM docker.io/giovtorres/slurm-docker-cluster:latest

RUN set -ex \
    && dnf -y install --enablerepo=epel tini \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Install uv (Python package manager used by ns-hpc)
ADD https://astral.sh/uv/install.sh /tmp/install-uv.sh
RUN set -ex \
    && bash /tmp/install-uv.sh \
    && rm /tmp/install-uv.sh \
    && cp /root/.local/bin/uv /usr/local/bin/uv \
    && rm -rf /root/.local/bin/uv \
    && uv --version

# Create unprivileged test user for non-root integration tests
RUN useradd -m -u 2000 -s /bin/bash testuser
