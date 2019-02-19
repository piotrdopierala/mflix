package mflix.lessons;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** @see com.mongodb.MongoWriteException */
@SpringBootTest
public class BasicWrites extends AbstractLesson {

  private MongoCollection<Document> videoGames;

  @Before
  public void setUp() {

    videoGames = testDb.getCollection("video_games");
  }

  @Test
  public void testWriteOneDocument() {

    // we set the first key, right in the constructor.
    Document doc = new Document("title", "Fortnite");

    doc.append("year", 2018);
    doc.put("label", "Epic Games");

    // then we can insert this document by calling the collection insertOne
    // method
    videoGames.insertOne(doc);

    Assert.assertNotNull(doc.getObjectId("_id"));

    Document retrieved = videoGames.find(Filters.eq("_id", doc.getObjectId("_id"))).first();

    // Which we can assert that it is true
    Assert.assertEquals(retrieved, doc);
  }

  @Test
  public void testInsertMany() {

    List<Document> someGames = new ArrayList<>();

    Document doc1 = new Document("title", "Hitman 2");
    doc1.put("year", 2018);
    doc1.put("label", "Square Enix");

    Document doc2 = new Document();
    HashMap<String, Object> documentValues = new HashMap<>();
    documentValues.put("title", "Tom Raider");
    documentValues.put("label", "Eidos");
    documentValues.put("year", 2013);
    doc2.putAll(documentValues);

    someGames.add(doc1);
    someGames.add(doc2);

    // and finally insert all of these documents using insertMany
    videoGames.insertMany(someGames);

    // If we look back into the object references we can see that the _id
    // fields are correctly set
    Assert.assertNotNull(doc1.getObjectId("_id"));
    Assert.assertNotNull(doc2.getObjectId("_id"));

    List<ObjectId> ids = new ArrayList<>();
    ids.add(doc1.getObjectId("_id"));
    ids.add(doc2.getObjectId("_id"));

    // And that we can retrieve them back.
    Assert.assertEquals(2, videoGames.countDocuments(Filters.in("_id", ids)));
  }

  @Test
  public void testUpsertDocument() {

    // Let's go ahead and instantiate our document
    Document doc1 = new Document("title", "Final Fantasy");
    doc1.put("year", 2003);
    doc1.put("label", "Square Enix");

    Bson query = new Document("title", "Final Fantasy");

    UpdateResult resultNoUpsert = videoGames.updateOne(query, new Document("$set", doc1));
    Assert.assertEquals(0, resultNoUpsert.getMatchedCount());
    Assert.assertNotEquals(1, resultNoUpsert.getModifiedCount());

    UpdateOptions options = new UpdateOptions();
    options.upsert(true);

    UpdateResult resultWithUpsert =
        videoGames.updateOne(query, new Document("$set", doc1), options);

    Assert.assertEquals(0, resultWithUpsert.getModifiedCount());
    Assert.assertNotNull(resultWithUpsert.getUpsertedId());
    Assert.assertTrue(resultWithUpsert.getUpsertedId().isObjectId());

    Bson updateObj1 =
        Updates.combine(
            Updates.set("title", "Final Fantasy 1"), Updates.setOnInsert("just_inserted", "yes"));

    query = Filters.eq("title", "Final Fantasy");

    UpdateResult updateAlreadyExisting = videoGames.updateOne(query, updateObj1, options);

    Document finalFantasyRetrieved =
        videoGames.find(Filters.eq("title", "Final Fantasy 1")).first();
    Assert.assertFalse(finalFantasyRetrieved.keySet().contains("just_inserted"));

    // on the other hand, if the document is not updated, but inserted
    Document doc2 = new Document("title", "CS:GO");
    doc2.put("year", 2018);
    doc2.put("label", "Source");

    Document updateObj2 = new Document();
    updateObj2.put("$set", doc2);
    updateObj2.put("$setOnInsert", new Document("just_inserted", "yes"));

    UpdateResult newDocumentUpsertResult =
        videoGames.updateOne(Filters.eq("title", "CS:GO"), updateObj2, options);

    // Then, we will see the field correctly set, querying the collection
    // using the upsertId field in the update result object
    Bson queryInsertedDocument = new Document("_id", newDocumentUpsertResult.getUpsertedId());

    Document csgoDocument = videoGames.find(queryInsertedDocument).first();

    Assert.assertEquals("yes", csgoDocument.get("just_inserted"));
  }

  @After
  public void tearDown() {
    this.videoGames.drop();
  }
}
