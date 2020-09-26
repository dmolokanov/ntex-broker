# -*- mode: dockerfile -*-
# You can override this `--build-arg BASE_IMAGE=...` to use different
# version of Rust or OpenSSL.
ARG BASE_IMAGE=ekidd/rust-musl-builder:stable

# Our first FROM statement declares the build environment.
FROM ${BASE_IMAGE} AS builder

# Add our source code.
ADD . ./

# Build our application.
RUN cargo build --release \
    && strip /home/rust/src/target/x86_64-unknown-linux-musl/release/ntex-broker

FROM busybox

ARG USER_ID=1000
RUN adduser -Du ${USER_ID} broker

ENV RUST_LOG=info

EXPOSE 1883/tcp

COPY --from=builder \
    /home/rust/src/target/x86_64-unknown-linux-musl/release/ntex-broker \
    /usr/local/bin/

USER broker

ENTRYPOINT ["/usr/local/bin/ntex-broker"]