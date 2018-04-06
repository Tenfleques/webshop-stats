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
    private static Integer getField(String field){
        final Integer DATE_FIELD = 1, PLATFORM_FIELD = 2, REFERER_FIELD = 3,
                ITEM_FIELD = 4, QUANTITY_FIELD = 5, PRICE_FIELD = 6;
        switch (field.toUpperCase()){
            case "DATE":
                return DATE_FIELD;
            case "PLATFORM":
            case "DEVICE":
            case "USER_AGENT":
                return PLATFORM_FIELD;
            case "ITEM":
            case "STOCK":
                return ITEM_FIELD;
            case "QUANTITY":
            case "ITEMS_SOLD":
                return QUANTITY_FIELD;
            case "PRICE":
                return PRICE_FIELD;
            default:
                return REFERER_FIELD;
        }
    }
    private static Integer getStatistic(String field){
        final Integer COUNT = 0, PURCHASES = 1, PURCHASES_VALUE = 2;
        switch (field.toUpperCase()){
            case "PURCHASES_VALUE":
            case "SALES_VALUE":
                return PURCHASES_VALUE;
            case "PURCHASES":
            case "SALES":
                return PURCHASES;
            default:
                return COUNT;
        }
    }
    public static void main(String[] args) {

        String usage = "you must supply the config.json file with this structure"
                +"\n{"
                + "\n\t \"name\" : \"no spaces, required: Name-of-program \","
                + "\n\t \"description\" : \"optional: program description \","
                + "\n\t \"key\" : \"required default[REFERER]: the statistic key, either of [DATE, PLATFORM/DEVICE, " +
                    "USER_AGENT, ITEM/STOCK, QUANTITY/ITEMS_SOLD, PRICE]\","
                + "\n\t \"statistic\" : \"required default[COUNT]: statistic to run, either of " +
                "[PURCHASES_VALUE/SALES_VALUE, PURCHASES/SALES] \","
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

        final Integer KEY_FIELD, STATISTIC_FIELD,ENDPOINT_PORT;
        String BROKERS = "";
        final String TOPIC, ENDPOINT;

        try {
            File configFile = new File(args[0]);
            String filePath = configFile.getAbsolutePath();
            FileReader file = new FileReader(filePath);

            JSONParser parser = new JSONParser();
            JSONObject config = (JSONObject) parser.parse(file);
            if(!config.containsKey("key")
                    || !config.containsKey("brokers")
                    || !config.containsKey("topic")
                    || !config.containsKey("restUrl")
                    || !config.containsKey("restPort")){
                throw new KeyNotFoundException("error in config.json, missing keys.");
            }

            KEY_FIELD = getField(config.get("key").toString());
            STATISTIC_FIELD = getStatistic(config.get("brokers").toString());
            TOPIC = config.get("topic").toString();
            ENDPOINT = config.get("restUrl").toString();
            ENDPOINT_PORT = Integer.parseInt(config.get("restPort").toString());

            for (Object broker: (JSONArray)config.get("brokers") ) {
                BROKERS += (BROKERS.length() == 0) ? "" : ",";
                BROKERS += broker;
            }



            // application can only run one stats at a given time
            // statistics available for referee agent
            try {
                new Aggregate(KEY_FIELD,BROKERS,TOPIC,STATISTIC_FIELD,ENDPOINT,ENDPOINT_PORT);
            }catch (Exception e ){

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
