package net.genealogy.wikibase;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class Performance {

    private final DatabaseInsert databaseInsert;
    private final Logger log = LoggerFactory.getLogger(Performance.class);
    private int propertyInstanceOf;
    private int itemTestEntry;
    private int propertyMyProperty;

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/my_wiki?characterEncoding=utf-8",
                "wikiuser",
                "sqlpass");

        DatabaseInsert di = new DatabaseInsert(connection);
        Performance self = new Performance(di);

        self.createPropertiesAndItems();
        self.runWithoutTransaction(100);
        self.runWithTransaction(10000);
        di.destroy();
    }

    private void runWithoutTransaction(final int numberOfItems) throws SQLException {
        final StopWatch stopWatch = new StopWatch();
        log.info("Creating {} items without a transaction...", numberOfItems);
        stopWatch.start();
        for (int i = 1; i < numberOfItems; i++) {
            final String json = createJson(i);
            databaseInsert.createItem(json);
        }
        stopWatch.stop();
        log.info("Created {} items in {} s.", numberOfItems, stopWatch.getTime(TimeUnit.SECONDS));
        log.info("Speed is {} items/minute.", (int) (numberOfItems * 60 / (double) stopWatch.getTime(TimeUnit.SECONDS)));
    }

    private void runWithTransaction(final int numberOfItems) throws SQLException {
        final StopWatch stopWatch = new StopWatch();
        log.info("Creating {} items with a transaction...", numberOfItems);
        stopWatch.start();
        databaseInsert.startTransaction();

        for (int i = 1; i < numberOfItems; i++) {
            final String json = createJson(i);
            databaseInsert.createItem(json);
        }

        databaseInsert.commit();
        stopWatch.stop();
        log.info("Created {} items in {} s.", numberOfItems, stopWatch.getTime(TimeUnit.SECONDS));
        log.info("Speed is {} items/minute.", (int) (numberOfItems * 60 / (double) stopWatch.getTime(TimeUnit.SECONDS)));

    }


    private String createJson(int i) {
        return "{\"type\": \"item\",\"labels\": {\"en\": {\"language\": \"en\",\"value\": \"Test " + i + "\"}}," +
                "\"descriptions\": {\"en\": {\"language\": \"en\",\"value\": \"Test " + i + "\"}}," +
                "\"aliases\": [],\"claims\": {" +
                "\"P" + propertyInstanceOf + "\": [{\"mainsnak\": {\"snaktype\": \"value\",\"property\": \"P" + propertyInstanceOf + "\",\"datavalue\": {" +
                "\"value\": {\"entity-type\": \"item\",\"numeric-id\": " + itemTestEntry + ",\"id\": \"Q" + itemTestEntry + "\"},\"type\": \"wikibase-entityid\"}},\"type\": \"statement\",\"rank\": \"normal\"}]," +
                "\"P" + propertyMyProperty + "\": [{\"mainsnak\": {\"snaktype\": \"value\",\"property\": \"P" + propertyMyProperty + "\",\"datavalue\": {\"value\": \"" + i + "\",\"type\": \"string\"}},\"type\": \"statement\",\"rank\": \"normal\"}]}," +
                "\"sitelinks\": []\n" +
                "}";
    }

    private void createPropertiesAndItems() throws SQLException {
        propertyInstanceOf = databaseInsert.findPropertyByLabel(Locale.ENGLISH, "instance of");
        if (propertyInstanceOf == 0) {

            System.err.println("Create a new property http://localhost:8181/wiki/Special:NewProperty with an English label 'instance of' and data type 'Item'.");
            System.exit(2);

            // TODO this does not work, yet
            // String json = "{\"type\":\"property\",\"datatype\":\"wikibase-item\",\"labels\":{\"en\":{\"language\":\"en\",\"value\":\"instance of\"}},\"descriptions\":[],\"aliases\":[],\"claims\":[]}";
            //String id = databaseInsert.createProperty(json);
            //propertyInstanceOf = Integer.parseInt(id.substring(1));
        } else {
            log.debug("Using P" + propertyInstanceOf + " as the instance-of property.");
        }

        propertyMyProperty = databaseInsert.findPropertyByLabel(Locale.ENGLISH, "my property");
        if (propertyMyProperty == 0) {
            System.err.println("Create a new property http://localhost:8181/wiki/Special:NewProperty with an English label 'my property' and data type 'External identifier'.");
            System.exit(2);

            // TODO this does not work, yet
            //  String json = "{\"type\":\"property\",\"datatype\":\"external-id\",\"labels\":{\"en\":{\"language\":\"en\",\"value\":\"my property\"}},\"descriptions\":[],\"aliases\":[],\"claims\":[]}";
            //  String id = databaseInsert.createProperty(json);
            //  propertyMyProperty = Integer.parseInt(id.substring(1));
        } else {
            log.debug("Using P" + propertyMyProperty + " as the data property.");
        }

        itemTestEntry = databaseInsert.findItemByLabel(Locale.ENGLISH, "test entry");
        if (itemTestEntry == 0) {

            System.err.println("Create a new item http://localhost:8181/wiki/Special:NewItem  with an English label 'test entry'.");

            // TODO this does not work, yet
            //String json = "{\"type\":\"item\",\"labels\":{\"de\":{\"language\":\"en\",\"value\":\"test entry\"}},\"descriptions\":{\"en\":{\"language\":\"en\",\"value\":\"type\"}},\"aliases\":[],\"claims\":[],\"sitelinks\":[]}";
            //String itemId = databaseInsert.createItem(json);
            //itemTestEntry = Integer.parseInt(itemId.substring(1));
        } else {
            log.debug("Using Q" + itemTestEntry + " as the type item.");
        }
    }

}