######################################################################################
#                                                                                    #
# To build: docker build -t ubuntu:scribengin directory                              # 
# To run:   docker run -t -i ubuntu:scribengin  /bin/bash                            #
#                                                                                    #
######################################################################################

FROM ubuntu:latest
MAINTAINER Nventdata 

RUN echo "Add ansible repo"
RUN apt-get install -y software-properties-common #Required for apt-add-repository
RUN apt-add-repository ppa:ansible/ansible
RUN apt-get update

RUN echo "Expose SSH port"
EXPOSE 22 2181 9092 50070 8088 9000 9300

RUN echo "Install openssh"
RUN apt-get install -y openssh-server openssh-client 

RUN echo "Install ansible"
RUN apt-get install -y ansible

RUN echo "Install basic packages"
RUN apt-get install -y vim nano wget rsync

RUN echo "Install OpenJDK7"
RUN apt-get install -y openjdk-7-jdk
RUN echo 'JAVA_HOME="/usr/lib/jvm/java-7-openjdk-amd64"' >> /etc/environment

RUN echo "Configure root Account"
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd

RUN echo "SSH login fix for Docker"
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile
RUN echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config


RUN echo "Setup ${account.dev} user"
RUN useradd -m -d /home/${account.dev} -s /bin/bash -c "${account.dev} user" -p $(openssl passwd -1 ${account.dev})  ${account.dev}
RUN echo "${account.dev} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN chown -R ${account.dev}:${account.dev} /opt


RUN echo "Sets up public keys for ssh.  Assumes your public keys are stored in ./ssh"
RUN mkdir -p /root/.ssh
RUN chmod 700 /root/.ssh
ADD ssh/authorized_keys /root/.ssh/authorized_keys
RUN chmod 644 /root/.ssh/authorized_keys

RUN mkdir -p /home/${account.dev}/.ssh
RUN chown -R ${account.dev}:${account.dev} /home/${account.dev}/.ssh
RUN chmod 700 /home/${account.dev}/.ssh
ADD ssh/authorized_keys /home/${account.dev}/.ssh/authorized_keys
RUN chmod 644 /home/${account.dev}/.ssh/authorized_keys

RUN echo "Start sshd service"
CMD ["/usr/sbin/sshd", "-D"]
