package net.genealogy.wikibase;

import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.datamodel.helpers.PropertyDocumentBuilder;
import org.wikidata.wdtk.datamodel.implementation.DatatypeIdImpl;
import org.wikidata.wdtk.datamodel.interfaces.PropertyDocument;
import org.wikidata.wdtk.datamodel.interfaces.PropertyIdValue;
import org.wikidata.wdtk.wikibaseapi.BasicApiConnection;
import org.wikidata.wdtk.wikibaseapi.LoginFailedException;
import org.wikidata.wdtk.wikibaseapi.WikibaseDataEditor;
import org.wikidata.wdtk.wikibaseapi.apierrors.MediaWikiApiErrorException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.wikidata.wdtk.datamodel.implementation.DatatypeIdImpl.JSON_DT_EXTERNAL_ID;
import static org.wikidata.wdtk.datamodel.implementation.DatatypeIdImpl.JSON_DT_ITEM;

@RequiredArgsConstructor
public class Performance {
    private final static String siteIri = "http://www.test.wikidata.org/entity/";

    private final DatabaseInsert databaseInsert;
    private final Logger log = LoggerFactory.getLogger(Performance.class);
    private int propertyInstanceOf;
    private int itemTestEntry;
    private int propertyMyProperty;
    private WikibaseDataEditor wbde;

    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/my_wiki?characterEncoding=utf-8",
                "wikiuser",
                "sqlpass");

        DatabaseInsert di = new DatabaseInsert(connection);
        Performance self = new Performance(di);

        BasicApiConnection wikibaseConnection = new BasicApiConnection("http://localhost:8181/w/api.php");
        wikibaseConnection.login("WikibaseAdmin", "WikibaseDockerAdminPass");
        self.wbde = new WikibaseDataEditor(wikibaseConnection, siteIri);
        self.createPropertiesAndItems();
        self.runWithoutTransaction(100);
        self.runWithTransaction(10000);
        di.destroy();
        System.exit(0);
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

    private int createPropertyIfNecessary(WikibaseDataEditor wbde, String englishLabel, String datatype) throws SQLException, IOException, MediaWikiApiErrorException {
        int propertyId = databaseInsert.findPropertyByLabel(Locale.ENGLISH, englishLabel);
        if (propertyId == 0) {
            final PropertyDocument newProperty =
                    PropertyDocumentBuilder.forPropertyIdAndDatatype(PropertyIdValue.NULL, DatatypeIdImpl.getDatatypeIriFromJsonDatatype(datatype))
                            .withLabel(englishLabel, "en")
                            .build();

            try {
                wbde.createPropertyDocument(newProperty,
                        "property for performance test",
                        Collections.emptyList());
            } catch (ValueInstantiationException ignore) {
                // The response cannot be parsed correctly
            }
        }
        return databaseInsert.findPropertyByLabel(Locale.ENGLISH, englishLabel);
    }

    private void createPropertiesAndItems() throws SQLException, LoginFailedException, IOException, MediaWikiApiErrorException {


        propertyInstanceOf = createPropertyIfNecessary(wbde, "instance of", JSON_DT_ITEM);
        log.debug("Using P" + propertyInstanceOf + " as the instance-of property.");

        propertyMyProperty = createPropertyIfNecessary(wbde, "my property", JSON_DT_EXTERNAL_ID);
        log.debug("Using P" + propertyMyProperty + " as the data property.");

        databaseInsert.afterPropertiesSet();

        itemTestEntry = databaseInsert.findItemByLabel(Locale.ENGLISH, "test entry");
        if (itemTestEntry == 0) {
            String json = "{\"type\":\"item\",\"labels\":{\"de\":{\"language\":\"en\",\"value\":\"test entry\"}},\"descriptions\":{\"en\":{\"language\":\"en\",\"value\":\"type\"}},\"aliases\":[]}";
            String itemId = databaseInsert.createItem(json);
            itemTestEntry = Integer.parseInt(itemId.substring(1));
        }
        log.debug("Using Q" + itemTestEntry + " as the type item.");

    }

}
