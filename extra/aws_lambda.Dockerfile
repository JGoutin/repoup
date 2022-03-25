# AWS Lambda container image sample
#
# Use Fedora as base image to have a recent GnuPG because it is too old
# (Version 2.0) on the Amazon linux 2 used in default lambda images.
# This also simplify the build or "createrepo_c" with all options and ensure RPM tools
# are the most up to date.

ARG FUNCTION_DIR="/var/task/"

FROM fedora AS build-image
ARG FUNCTION_DIR
WORKDIR ${FUNCTION_DIR}
RUN dnf install -yq \
      bzip2-devel \
      cmake \
      drpm-devel \
      file-devel \
      findutils \
      gcc-c++ \
      glib2-devel \
      libcurl-devel \
      libmodulemd-devel \
      libxml2-devel \
      ninja-build \
      openssl-devel \
      python-devel \
      python-pip \
      rpm-devel \
      sqlite-devel \
      xz-devel \
      zchunk-devel \
      zlib-devel \
 && pip install -q --no-cache-dir --disable-pip-version-check -t . \
      awslambdaric \
      repoup[aws,deb,rpm,speedups]
RUN rm -rf bin \
 && python -m compileall . -q -b -j0 -o2 \
 && find . -type f -name '*.py' -delete \
 && find . -type d -name __pycache__ -exec rm -rf {} +

FROM debian AS debsigs-image
RUN apt update -qq \
 && cd /opt \
 && apt download debsigs \
 && dpkg -x debsigs_* . \
 && rm -rf /opt/usr/share/doc /opt/usr/share/man *.deb

FROM fedora
ARG FUNCTION_DIR
WORKDIR ${FUNCTION_DIR}
RUN dnf -yq install \
      binutils \
      gnupg \
      python-apt \
      rpm-sign \
 && dnf clean -yq all
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}
COPY --from=debsigs-image /opt/usr /usr
ENTRYPOINT ["/usr/bin/python3", "-m", "awslambdaric"]
CMD ["repoup.entrypoint.aws_lambda.handler"]
