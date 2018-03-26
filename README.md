# BigDataSchool2018
Project by Maxim and Tendai. 
The systems create statistics for a web shop selling phones

Программа минимум:  
    1. Получение данных: два генератора  
        a. Генератор ссылок от рефералов с информацией(распространитель, платформа пользователя, время …)  
        b. Генератор действий пользователя  
    2. Брокер сообщений: kafka с двумя топиками:  
        a. Сообщения от рефералов  
        b. Сообщения о действиях пользователей  
    3. SPARK Streaming используется для подсчета общего количества переходов по реферальным ссылкам в данный момент  
    4. Периодический анализ и выявление фейковых переходов и процент эффективных пользователей  
 Программа максимум:  
    1. Получение данных: Сайт  
    2. Брокер сообщений: kafka с двумя топиками:  
        a. Сообщения от рефералов  
        b. Сообщения о действиях пользователей  
    3. SPARK Streaming используется для подсчета общего количества переходов по реферальным ссылкам в данный момент  
    4. Периодический анализ и выявление фейковых переходов и процент эффективных пользователей, выявлять тенденции у пользователей, вычислять коэффициент полезности рекламы у конкретного распространителя   
    
    Running Programs:  
    1. start Zookeeper and Kafka 
    2. to stream pipe data build the with mvn the Pipe program: mvn clean package  
        mvn exec:java -Dexec.mainClass="bigdata.project.Pipe"  
       run the listener for the pipe  
       /path/to/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clients-purchases-output  
       run the purchases simulator
       node /path/to/clients-simulator/index.js  
       watch the csv rows piped to Kafka
      
        
