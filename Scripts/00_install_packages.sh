cd /tmp/
wget https://chromedriver.storage.googleapis.com/2.37/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/bin/chromedriver
chromedriver --version
cp /usr/bin/chromedriver /home/ec2-user/dpa_equipo2/Scripts

curl https://intoli.com/install-google-chrome.sh | bash
sudo mv /usr/bin/google-chrome-stable /usr/bin/google-chrome
google-chrome --version && which google-chrome

#cd /usr/bin/
#ls

#sudo yum install python36

#sudo alternatives --set python /usr/bin/python3.6
#python --version

#cd /tmp
#curl -O https://bootstrap.pypa.io/get-pip.py
#python3 get-pip.py --user
#pip3 --version

#pip3 install selenium --user
#pip3 install luigi
#pip3 install boto3
#pip3 install numpy
#pip3 install -U scikit-learn
#pip3 install marbles

#sudo yum update
#sudo yum install postgresql postgresql-contrib

#pip3 install psycopg2-binary
#pip3 install click
#pip3 install dynaconf
#pip3 install pandas
#pip3 install flask
#pip3 install flask-restplus


export LC_ALL=en_US.UTF-8

# programar ejecución periódica de la  descarga re
# Actualizar permisos del script
chmod 700 Descarga_Recurrente.sh
# sudo cp Descarga_Recurrente.sh /etc/cron.weekly
#Programar d�ia y hora de ejeucion
echo "30 22 * * 4 root /home/ec2-user/dpa_equipo2/Scripts/DescargaRecurrente.sh" | sudo tee -a /etc/crontab
