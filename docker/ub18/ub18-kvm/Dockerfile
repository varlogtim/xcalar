ARG UB18=ubuntu:bionic
FROM $UB18

ARG KVM_GROUP=139

RUN groupadd -g 999 -r docker \
    && groupadd -g $KVM_GROUP -o -f -r kvm \
    && apt-get update -q \
        && apt install --no-install-recommends -y qemu-system-x86 qemu-kvm libvirt-bin bridge-utils libguestfs-tools sudo \
        && apt clean all \
        && rm -rf /var/apt/cache/lists/*


RUN mkdir -p /etc/sudoers.d && echo '%sudo ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/99-sudo && chmod 0440 /etc/sudoers.d/99-sudo
RUN if [ -n "$CONTAINER_USER" ] && [ -n "$CONTAINER_UID" ] && ! grep -q "^$CONTAINER_USER:x:$CONTAINER_UID:" /etc/passwd; then useradd -m -s /bin/bash -u $CONTAINER_UID $CONTAINER_USER; fi  # docker_only
RUN if [ -n "$CONTAINER_USER" ]; then usermod -aG sudo,docker,disk,kvm $CONTAINER_USER; fi


CMD ["/bin/bash","-l"]
