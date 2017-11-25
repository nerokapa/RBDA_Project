# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.define "rbda" do |rbda|
      rbda.vm.box = "ubuntu/xenial64"
      rbda.vm.network "private_network", ip: "192.168.33.10"

      rbda.vm.synced_folder "./", "/vagrant", owner: "ubuntu", mount_options: ["dmode=755,fmode=644"]

      rbda.vm.provider "virtualbox" do |vb|
        # Customize the amount of memory on the VM:
        vb.memory = "2048"
        vb.cpus = 2
      end
  end

  config.vm.provision "shell", inline: <<-SHELL
    apt-get update
    apt-get install -y git python-pip python-dev build-essential
    pip install --upgrade pip
    apt-get -y autoremove
    # Install app dependencies
    cd /vagrant
    sudo pip install -r requirements.txt
    python download_nltk_model.py

  SHELL

  # install hbase
  config.vm.provision "shell", privileged: false, path: "install_hbase.sh"

end
