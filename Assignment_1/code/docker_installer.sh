#!/bin/sh
# Updates
sudo apt update; sudo apt upgrade -y

## Installing Docker
# Installing a few prerequisite packages which let apt use packages over HTTPS:
sudo apt install apt-transport-https ca-certificates curl software-properties-common -y
# Then add the GPG key for the official Docker repository to your system:
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
# Add the Docker repository to APT sources:
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
# Update your existing list of packages again for the addition to be recognized:
sudo apt update
# Installing Docker:
sudo apt install docker-ce -y
# Check that it’s running:
# sudo systemctl status docker

## Installing Docker Compose
# Confirm the latest version available in their releases page. At the time of this writing, the most current stable version is 2.3.3
mkdir -p ~/.docker/cli-plugins/
curl -SL https://github.com/docker/compose/releases/download/v2.3.3/docker-compose-linux-x86_64 -o ~/.docker/cli-plugins/docker-compose
# Next, set the correct permissions so that the docker compose command is executable:
chmod +x ~/.docker/cli-plugins/docker-compose
# To verify that the installation was successful, you can run:
# docker compose version

## Install python3-pip
sudo apt install python3-pip -y