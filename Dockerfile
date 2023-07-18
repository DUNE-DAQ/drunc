# Use Python 3.10.4-bullseye as a base image
FROM python:3.10.4-bullseye AS builder

# Define shell
SHELL ["/bin/bash", "-c"]

# Update the packages list and install the required packages
RUN apt update && apt install -y --no-install-recommends \
    git \
    openssh-server \
    openssh-client

# Branch applied
# Clone the repository and install the required Python packages
RUN git clone -b plasorak/better-process-queries https://github.com/DUNE-DAQ/drunc.git && \
    cd drunc && \
    pip install -r requirements.txt && \
    pip install .

# Start a new build stage with Python 3.10.4-bullseye as the base image
FROM python:3.10.4-bullseye

# Copy everything from the builder stage
COPY --from=builder / /

# Generate root ssh key
RUN mkdir -p /root/.ssh && \
    ssh-keygen -b 2048 -t rsa -f /root/.ssh/id_rsa -q -N "" && \
    cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# Update the SSHD configuration to allow root login without password
RUN echo "PermitRootLogin without-password" >> /etc/ssh/sshd_config && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config

# Define environment variables
ENV DRUNC_DIR=/drunc
ENV DRUNC_DATA=/drunc/data

# Expose SSH default port
EXPOSE 22

# Run SSHD in the background and drunc-process-manager in the foreground
ENTRYPOINT service ssh start && drunc-process-manager --loglevel debug

