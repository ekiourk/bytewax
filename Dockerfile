ARG PYTHON_VERSION=3.12

FROM rust:1.83-slim-bookworm AS build

ARG PYTHON_VERSION
ARG BYTEWAX_VERSION

RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes \
        build-essential \
        cmake \
        gcc \
        libpython3-dev \
        libsasl2-dev \
        libssl-dev \
        make \
        openssl \
        patchelf \
        pkg-config \
        protobuf-compiler \
        python3-venv && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel maturin

COPY . /bytewax
WORKDIR /bytewax

RUN /venv/bin/maturin build --release --interpreter python${PYTHON_VERSION}
RUN /venv/bin/pip3 install /bytewax/target/wheels/bytewax-${BYTEWAX_VERSION}-*.whl

# Final image
FROM gcr.io/distroless/python3-debian12:debug
COPY --from=build /venv /venv
WORKDIR /bytewax
COPY ./entrypoint.sh .
COPY ./entrypoint-recovery.sh .

ENV BYTEWAX_WORKDIR=/bytewax

# Ports that needs to be exposed
EXPOSE 9999 3030

ENTRYPOINT ["/bin/sh", "-c", "./entrypoint.sh"]
