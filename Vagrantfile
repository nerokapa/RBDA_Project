# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
cd ~
mkdir tools
cd tools
sudo apt install -y openjdk-8-jre
wget -N -c  http://apache.claz.org/hbase/1.3.1/hbase-1.3.1-bin.tar.gz
tar xvzf hbase-1.3.1-bin.tar.gz

cat << 'EOF' >> ~/.profile
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export HBASE_HOME=/home/ubuntu/tools/hbase-1.3.1
export PATH=\$PATH:\$HBASE_HOME/bin
EOF
rm hbase-1.3.1-bin.tar.gz

sudo pip install py4j
sudo wget http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.4.tgz
sudo tar -xvzf /vagrant/resource/spark-1.5.1-bin-hadoop2.4.tgz
sudo chown -R ubuntu spark-1.5.1-bin-hadoop2.4
sudo chgrp -R ubuntu spark-1.5.1-bin-hadoop2.4
sudo cp /vagrant/resource/log4j.properties /home/ubuntu/tools/spark-1.5.1-bin-hadoop2.4/conf

cat << 'EOF' >> ~/.bashrc
export SPARK_HOME=/home/ubuntu/tools/spark-1.5.1-bin-hadoop2.4
export PATH=$SPARK_HOME/bin:$PATH
export PATH=\$PATH:\$HBASE_HOME/bin
EOF
rm spark-1.5.1-bin-hadoop2.4.tgz
sudo pip install pyspark --no-cache-dir
SCRIPT



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
    sudo pip install ipython
    python download_nltk_model.py
  SHELL

  # install hbase
  config.vm.provision "shell", privileged: false, inline:$script

end
