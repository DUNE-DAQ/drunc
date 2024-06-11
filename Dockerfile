# Use Python 3.10.4-bullseye as a base image
FROM python:3.10.4-bullseye

# Define shell
SHELL ["/bin/bash", "-c"]

# Update the packages list and install the required packages
RUN apt-get update

RUN apt-get install -y --no-install-recommends \
    git \
    openssh-server \
    openssh-client \
    net-tools \
    sudo

RUN cd /etc/ssh/ && ssh-keygen -A

RUN echo 'root:r00t' | chpasswd

# Update the SSHD configuration to allow root login without password
RUN printf "\nPermitRootLogin without-password\n" >> /etc/ssh/sshd_config && \
    printf "Port 23\n" >> /etc/ssh/sshd_config

RUN printf "    Port 23\n" >> /etc/ssh/ssh_config && \
    printf "    StrictHostKeyChecking no\n" >> /etc/ssh/ssh_config && \
    printf "    PasswordAuthentication no\n" >> /etc/ssh/ssh_config && \
    printf "    UserKnownHostsFile /dev/null\n" >> /etc/ssh/ssh_config && \
    printf "    AddressFamily inet" >> /etc/ssh/ssh_config

RUN mkdir /run/sshd

RUN adduser --shell /bin/bash --ingroup sudo patreides
RUN echo 'patreides:chani' | chpasswd
RUN usermod -aG sudo patreides

RUN service ssh start

# Expose SSH port (which was changed from 22 to 23 a couple of lines above)
EXPOSE 23
EXPOSE 10054

USER patreides

WORKDIR /home/patreides

RUN cd /home/patreides && \
    git clone -b develop https://github.com/DUNE-DAQ/druncschema.git && \
    cd druncschema && \
    pip install -r requirements.txt && \
    pip install .

RUN cd /home/patreides && \
    git clone -b develop https://github.com/DUNE-DAQ/drunc.git && \
    cd drunc && \
    pip install -r requirements.txt && \
    pip install .

RUN mkdir /home/patreides/.ssh
RUN ssh-keygen -f /home/patreides/.ssh/id_rsa -q -N ""
RUN cp /home/patreides/.ssh/id_rsa.pub /home/patreides/.ssh/authorized_keys

# Run SSHD in the background
#ENTRYPOINT echo 'chani' | sudo -S service ssh start

