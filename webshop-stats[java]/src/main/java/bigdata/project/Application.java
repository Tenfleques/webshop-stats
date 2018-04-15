package bigdata.project;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

//19,1499882547,Platform e.g Android 4.0.2,instagram.*,HTC Desire HD,0,5000
public class Application {
    /*
    * required topic
    * statistic, brokers
    * endpoint
    *
     */
    public static void main(String[] args) {

        String usage = "you must supply the config.json file with this structure"
                +"\n{"
                + "\n\t \"brokers\" : [\"required: the clusters address e.g localhost:9092\"],"
                + "\n\t \"topic\" : \"required: the Kafka topic feeding the application\","
                + "\n\t \"restUrl\" : \"required: the url for the REST endpoint e.g localhost\","
                + "\n\t \"restPort\" : \"required: the port for the REST endpoint e.g 7070\""
                + "\n}\n";


        if(args.length != 1) {
            System.out.print(usage);
            System.exit(1);
            return;
        }

        final Integer ENDPOINT_PORT, REDIS_PORT;
        String BROKERS = "";
        final String TOPIC, ENDPOINT, REDIS_URL;

        try {
            File configFile = new File(args[0]);
            String filePath = configFile.getAbsolutePath();
            FileReader file = new FileReader(filePath);

            JSONParser parser = new JSONParser();
            JSONObject config = (JSONObject) parser.parse(file);
            if(!config.containsKey("brokers")
                    || !config.containsKey("topic")
                    || !config.containsKey("restUrl")
                    || !config.containsKey("restPort")
                    || !config.containsKey("redisUrl")
                    || !config.containsKey("redisPort")){
                throw new KeyNotFoundException("error in config.json, missing keys.");
            }
            TOPIC = config.get("topic").toString();
            ENDPOINT = config.get("restUrl").toString();
            ENDPOINT_PORT = Integer.parseInt(config.get("restPort").toString());

            REDIS_URL = config.get("redisUrl").toString();
            REDIS_PORT = Integer.parseInt(config.get("redisPort").toString());

            for (Object broker: (JSONArray)config.get("brokers") ) {
                BROKERS += (BROKERS.length() == 0) ? "" : ",";
                BROKERS += broker;
            }
            try {
                new AggregateKafka(BROKERS,TOPIC,ENDPOINT,ENDPOINT_PORT,REDIS_URL,REDIS_PORT);
            }catch (Exception e ){
                System.out.print(e.getMessage());
                e.printStackTrace();
            }

        }catch (FileNotFoundException e){
            System.out.print("the config.json file was not found, verify if you have supplied the correct path to the" +
                    " file");
        }catch (IOException e){
            System.out.print("failed to open the file supplied. check to see if you have the right permissions or " +
                    "that the file is not locked by another process" +
                    " ");
        }catch(ParseException e) {
            System.out.print("Failed to parse the config.json supplied, check if you have supplied a correct path to " +
                    "the config.json file and that it is in the exact format!");
            System.out.print(usage);
        }catch (KeyNotFoundException e){
            System.out.print(e + usage);
        }
    }
}
