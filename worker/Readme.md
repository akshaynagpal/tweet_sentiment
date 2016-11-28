~/cloud/credentials    
ssh -i "file.pem" ec2_url    

sudo apt-get update -y    
sudo apt-get upgrade -y    
sudo add-apt-repository -y ppa:webupd8team/java    
sudo apt-get update -y    
sudo apt-get install oracle-java8-installer -y    
java -version    
sudo apt-get install zookeeperd    
netstat -ant | grep :2181    
wget http://apache.claz.org/kafka/0.10.1.0/kafka_2.11-0.10.1.0.tgz    
sudo mkdir /opt/Kafka    
cd /opt/Kafka    
sudo tar -xvf kafka_2.11-0.10.1.0.tgz -C /opt/Kafka/    
sudo vi /opt/Kafka/kafka_2.11-0.10.1.0/bin/kafka-server-start.sh     
// edit this line export KAFKA_HEAP_OPTS="-Xmx512m -Xms512m"    

sudo nohup /opt/Kafka/kafka_2.11-0.10.1.0/bin/kafka-server-start.sh /opt/Kafka/kafka_2.11-0.10.1.0/config/server.properties > /tmp/kafka.log 2>&1 &    

java -jar streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar    
java -jar worker-0.0.1-SNAPSHOT-jar-with-dependencies.jar    