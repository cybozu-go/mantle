FROM ghcr.io/cybozu/ubuntu:22.04 AS build-ceph

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV CEPH_TAG=v19.2.1
ENV WORKSPACE=/workspace

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    build-essential curl git

# Workaround for github actions runner.
# Ceph depends on this library, but it is not automatically installed
# because libraries that conflict with this library are installed.
# Therefore, it should be installed explicitly.
# See. https://github.com/actions/runner-images/issues/6399#issuecomment-1286050292
RUN apt install --no-install-recommends -y libunwind-dev

WORKDIR ${WORKSPACE}
RUN git clone -b ${CEPH_TAG} --depth=1 --recurse-submodules --shallow-submodules https://github.com/ceph/ceph.git

COPY *.patch ./
WORKDIR ${WORKSPACE}/ceph

RUN ./install-deps.sh && \
    apt install --no-install-recommends -y python3-routes

# Addition of debian pkg in ceph exporter was omitted in squid.
# https://github.com/ceph/ceph/pull/56541
RUN patch -p1 < ${WORKSPACE}/exporter.patch

# This patch customizes the ceph rbd export-diff command. 
RUN patch -p1 < ${WORKSPACE}/export-diff.patch

# Prebuild ceph source to generate files in `src/pybind/mgr/dashboard/frontend/dist` needed by CMake
RUN ./make-dist

# Build Ceph packages
RUN sed -i -e 's/WITH_CEPHFS_JAVA=ON/WITH_CEPHFS_JAVA=OFF/' debian/rules && \
    sed -i -e 's@usr/share/java/libcephfs-test.jar@@' debian/ceph-test.install
RUN rm debian/libcephfs-java.jlibs debian/libcephfs-jni.install debian/ceph-mgr-dashboard*
RUN dpkg-buildpackage --build=binary -uc -us -j10
RUN rm ${WORKSPACE}/*-dbg_*.deb ${WORKSPACE}/ceph-test_*.deb

FROM ghcr.io/cybozu/ubuntu:22.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV WORKSPACE=/workspace
ENV PACKAGES=/packages

# Since the package file is extracted outside the docker image at the end, keep the files.
COPY --from=build-ceph ${WORKSPACE}/*.deb ${WORKSPACE}/ceph/COPYING* ${PACKAGES}/

WORKDIR ${WORKSPACE}
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/usr/local/bin/kubectl"
RUN mkdir -p /var/run/ceph && \
    apt-get update && apt-get install --no-install-recommends -y \
        libstdc++-11-dev jq kmod lvm2 gdisk ca-certificates e2fsprogs attr udev libgflags2.2 ${PACKAGES}/*.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    sed -i -e 's/udev_rules = 1/udev_rules = 0/' -e 's/udev_sync = 1/udev_sync = 0/' -e 's/obtain_device_list_from_udev = 1/obtain_device_list_from_udev = 0/' /etc/lvm/lvm.conf && \
    # validate the sed command worked as expected
    grep -sqo "udev_sync = 0" /etc/lvm/lvm.conf && \
    grep -sqo "udev_rules = 0" /etc/lvm/lvm.conf && \
    grep -sqo "obtain_device_list_from_udev = 0" /etc/lvm/lvm.conf && \
    # Clean common files like /tmp, /var/lib, etc.
    rm -rf \
        /etc/{selinux,systemd,udev} \
        /lib/{lsb,udev} \
        /tmp/* \
        /usr/lib{,64}/{locale,systemd,udev,dracut} \
        /usr/share/{doc,info,locale,man} \
        /usr/share/{bash-completion,pkgconfig/bash-completion.pc} \
        /var/log/* \
        /var/tmp/* && \
    find / -xdev \( -name "*.pyc" -o -name "*.pyo" \) -exec rm -f {} \; && \
    mkdir -p /usr/local/share/doc/ceph && \
    chown ceph:ceph -R /run/ceph /var/lib/ceph
