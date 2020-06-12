package net.genealogy.wikibase;


import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Mit diesem Programm soll versucht werden, ein Item direkt in der Wikibase-Datenbank anzulegen.
 */
public class DatabaseInsert {

    private static final Logger log = LoggerFactory.getLogger(DatabaseInsert.class);
    private final static int ACTOR = 1;
    PreparedStatement pstmtInsertText;
    PreparedStatement pstmtInsertPage;
    PreparedStatement pstmtInsertRevision;
    PreparedStatement pstmtInsertComment;
    PreparedStatement pstmtInsertRevisionComment;
    PreparedStatement pstmtInsertRevisionActor;
    PreparedStatement pstmtInsertContent;
    PreparedStatement pstmtInsertSlots;
    PreparedStatement pstmtUpdateWbIdCounters;
    PreparedStatement pstmtSelectLastItemId;
    Connection connection;
    PrintWriter sqlout;
    Map<String, Long> timer = new HashMap<>();
    int count = 0;
    private PreparedStatement pstmtSelectItem;
    private PreparedStatement pstmtInsertRecentChanges;

    /**
     * Do not read ids from the database for every item. Assign them once and assume no other process writes to the
     * database.
     */
    private boolean preselectIds = true;
    private int lastQNumber = 0;
    private long textId;
    private long pageId;
    private long commentId;
    private long contentId;

    public DatabaseInsert(Connection con) throws SQLException, IOException {
        this.connection = con;
        afterPropertiesSet();
        sqlout = new PrintWriter(new FileWriter("/tmp/wikibase.sql"));
    }

    private static String sha1base36(String s) {
        return new BigInteger(DigestUtils.sha1Hex(s), 16).toString(36);
    }


    public void afterPropertiesSet() throws SQLException {
        prepareDatabaseConnection();
    }

    public void destroy() throws Exception {
        connection.close();
        sqlout.close();
    }

    private void work(InputStream stream) throws SQLException, IOException {
        final BufferedReader in = new BufferedReader(new InputStreamReader(stream));
        String line = in.readLine();
        while (line != null) {
            createItem(line);
            line = in.readLine();
        }

        in.close();
    }

    private int findFirstTermByLabel(String type, Locale language, String name) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement("SELECT min(substring(term_full_entity_id,2)) FROM wb_terms WHERE term_language=? AND term_entity_type=? AND term_type='label' AND term_text=?");
        pstmt.setString(1, language.toLanguageTag());
        pstmt.setString(2, type);
        pstmt.setString(3, name);
        ResultSet rs = pstmt.executeQuery();

        int termNumber;

        if (rs.next()) {
            termNumber = rs.getInt(1);
        } else {
            termNumber = 0;
        }

        rs.close();
        pstmt.close();

        return termNumber;
    }

    void startTransaction() throws SQLException {
        connection.setAutoCommit(false);
    }

    void commit() throws SQLException {
        connection.commit();
    }

    int findPropertyByLabel(Locale language, String name) throws SQLException {
        return findFirstTermByLabel("property", language, name);
    }

    int findItemByLabel(Locale language, String name) throws SQLException {
        return findFirstTermByLabel("item", language, name);
    }

    private void prepareDatabaseConnection() throws SQLException {

        if (preselectIds) {
            pstmtInsertText = connection.prepareStatement("INSERT INTO text VALUES(?,?,'utf-8')");
            pstmtInsertPage = connection.prepareStatement("INSERT INTO page VALUES(?,120,?,'',0,0,rand(1),?,?,?,?,'wikibase-item',NULL)");
            pstmtInsertComment = connection.prepareStatement("INSERT INTO comment VALUES(?,?,?,NULL)");
            pstmtInsertContent = connection.prepareStatement("INSERT INTO content VALUES( ? ,?,?, 2, ?)");
        } else {
            pstmtInsertText = connection.prepareStatement("INSERT INTO text VALUES(?,?,'utf-8')", Statement.RETURN_GENERATED_KEYS);
            pstmtInsertPage = connection.prepareStatement("INSERT INTO page VALUES(?,120,?,'',0,0,rand(1),?,?,?,?,'wikibase-item',NULL)", Statement.RETURN_GENERATED_KEYS);
            pstmtInsertComment = connection.prepareStatement("INSERT INTO comment VALUES(?,?,?,NULL)", Statement.RETURN_GENERATED_KEYS);
            pstmtInsertContent = connection.prepareStatement("INSERT INTO content VALUES( ? ,?,?, 2, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        pstmtInsertRevisionComment = connection.prepareStatement("INSERT INTO revision_comment_temp VALUES (?,?)");
        pstmtInsertRevisionActor = connection.prepareStatement("INSERT INTO revision_actor_temp VALUES( ?, ?, ?,  ?)");
        pstmtInsertRevision = connection.prepareStatement("INSERT INTO revision VALUES(?,?,?,'',0,'',?,0,0,?,0,?,NULL,NULL)");
        pstmtInsertSlots = connection.prepareStatement("INSERT INTO slots VALUES( ?, 1, ?, ?)");
        pstmtUpdateWbIdCounters = connection.prepareStatement("UPDATE wb_id_counters SET id_value=? WHERE id_type='wikibase-item'");
        pstmtSelectLastItemId = connection.prepareStatement("SELECT id_value  AS next_id from wb_id_counters where id_type = 'wikibase-item'");
        pstmtSelectItem = connection.prepareStatement("SELECT * FROM page WHERE page_namespace=120 AND page_title=?");
        pstmtInsertRecentChanges = connection.prepareStatement("INSERT INTO recentchanges VALUES ( 0,?,0,'',?,120,?,?,0,0,1,?,?,0,1,'mw.new',0,'127.0.0.1',0,?,0,0,NULL,'',''  )");

        if (preselectIds) {
            ResultSet rs = pstmtSelectLastItemId.executeQuery();
            if (rs.next()) {
                lastQNumber = rs.getInt(1);
            }
            rs.close();

            // Check if the Q-number is really unused
            while (itemExists("Q" + (lastQNumber + 1))) {
                lastQNumber++;
            }

            final Statement stmt = connection.createStatement();
            rs = stmt.executeQuery("SELECT max(page_id) FROM page");
            rs.next();
            pageId = rs.getLong(1);
            rs.close();

            rs = stmt.executeQuery("SELECT max(old_id) FROM text");
            rs.next();
            textId = rs.getLong(1);
            rs.close();

            rs = stmt.executeQuery("SELECT max(comment_id) FROM comment");
            rs.next();
            commentId = rs.getLong(1);
            rs.close();

            rs = stmt.executeQuery("SELECT max(content_id) FROM content");
            rs.next();
            contentId = rs.getLong(1);
            rs.close();

            stmt.close();
        }
    }

    /**
     * Check if the specified item id exists.
     *
     * @param itemId a Q id
     * @return <code>true</code> if the item exists in the Wikibase database
     */
    public boolean itemExists(String itemId) throws SQLException {
        pstmtSelectItem.setString(1, itemId);
        final ResultSet rs = pstmtSelectItem.executeQuery();
        boolean result = rs.next();
        rs.close();
        return result;
    }


    public String createItem(String jsonString) throws SQLException {

        final JSONObject json = new JSONObject(jsonString);

        final String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME).replaceAll("[T:-]", "").substring(0, 14);

        if (preselectIds) {
            lastQNumber++;

        } else {
            final ResultSet rs = pstmtSelectLastItemId.executeQuery();
            rs.next();
            lastQNumber = rs.getInt(1) + 1;
            rs.close();
        }


        final String itemId = "Q" + lastQNumber;

        // Does the specified item already have an ID?
        if (!json.has("id")) {
            json.put("id", itemId);

            // All statements also need this ID
            final JSONObject claims = json.getJSONObject("claims");
            for (String claim : claims.keySet()) {
                final JSONArray list = claims.getJSONArray(claim);
                for (int i = 0; i < list.length(); i++) {
                    list.getJSONObject(i).put("id", itemId + "$" + UUID.randomUUID().toString());
                }
            }
        }

        final String data = json.toString();
        pstmtInsertText.setString(2, data);

        if (preselectIds) {
            textId++;
            pstmtInsertText.setLong(1, textId);
            executeUpdate(pstmtInsertText);
        } else {
            pstmtInsertText.setLong(1, 0);
            executeUpdate(pstmtInsertText);
            final ResultSet rs = pstmtInsertText.getGeneratedKeys();
            rs.next();
            textId = rs.getLong(1);
            rs.close();
        }

        pstmtInsertPage.setString(2, itemId);
        pstmtInsertPage.setString(3, timestamp);
        pstmtInsertPage.setString(4, timestamp);
        pstmtInsertPage.setLong(5, textId);
        pstmtInsertPage.setInt(6, data.length());

        if (preselectIds) {
            pageId++;
            pstmtInsertPage.setLong(1, pageId);
            executeUpdate(pstmtInsertPage);
        } else {
            pstmtInsertPage.setLong(1, 0);
            executeUpdate(pstmtInsertPage);
            final ResultSet rs = pstmtInsertPage.getGeneratedKeys();
            rs.next();
            pageId = rs.getLong(1);
            rs.close();
        }

        pstmtInsertRevision.setLong(1, textId);
        pstmtInsertRevision.setLong(2, pageId);
        pstmtInsertRevision.setLong(3, textId);
        pstmtInsertRevision.setString(4, timestamp);
        pstmtInsertRevision.setInt(5, data.length());
        pstmtInsertRevision.setString(6, sha1base36(data));
        executeUpdate(pstmtInsertRevision);

        final String comment = "/* wbeditentity-create:2|de */ " + itemId;

        pstmtInsertComment.setInt(2, comment.hashCode());
        pstmtInsertComment.setString(3, comment);

        if (preselectIds) {
            commentId++;
            pstmtInsertComment.setLong(1, commentId);
            executeUpdate(pstmtInsertComment);
        } else {
            pstmtInsertComment.setLong(1, 0);
            executeUpdate(pstmtInsertComment);
            final ResultSet rs = pstmtInsertComment.getGeneratedKeys();
            rs.next();
            commentId = rs.getLong(1);
            rs.close();
        }

        pstmtInsertRevisionComment.setLong(1, textId);
        pstmtInsertRevisionComment.setLong(2, commentId);
        executeUpdate(pstmtInsertRevisionComment);

        pstmtInsertRevisionActor.setLong(1, textId);
        pstmtInsertRevisionActor.setInt(2, ACTOR);
        pstmtInsertRevisionActor.setString(3, timestamp);
        pstmtInsertRevisionActor.setLong(4, pageId);
        executeUpdate(pstmtInsertRevisionActor);

        pstmtInsertContent.setInt(2, data.length());
        pstmtInsertContent.setString(3, sha1base36(data));
        pstmtInsertContent.setString(4, "tt:" + textId);

        if (preselectIds) {
            contentId++;
            pstmtInsertContent.setLong(1, contentId);
            executeUpdate(pstmtInsertContent);
        } else {
            pstmtInsertContent.setLong(1, 0);
            executeUpdate(pstmtInsertContent);
            final ResultSet rs = pstmtInsertContent.getGeneratedKeys();
            rs.next();
            contentId = rs.getLong(1);
            rs.close();
        }

        pstmtInsertSlots.setLong(1, textId);
        pstmtInsertSlots.setLong(2, contentId);
        pstmtInsertSlots.setLong(3, textId);
        executeUpdate(pstmtInsertSlots);

        pstmtInsertRecentChanges.setString(1, timestamp);
        pstmtInsertRecentChanges.setInt(2, ACTOR);
        pstmtInsertRecentChanges.setString(3, itemId);
        pstmtInsertRecentChanges.setLong(4, commentId);
        pstmtInsertRecentChanges.setLong(5, textId);
        pstmtInsertRecentChanges.setLong(6, textId);
        pstmtInsertRecentChanges.setInt(7, data.length());
        executeUpdate(pstmtInsertRecentChanges);

        pstmtUpdateWbIdCounters.setInt(1, lastQNumber);
        executeUpdate(pstmtUpdateWbIdCounters);

        return itemId;
    }

    private void executeUpdate(final PreparedStatement pstmt) throws SQLException {

        // Here you have a chance to log the executed statement.

        pstmt.executeUpdate();
    }
}
