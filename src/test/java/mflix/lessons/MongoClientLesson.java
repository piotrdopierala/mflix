package mflix.lessons;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @see com.mongodb.client.MongoClients
 * @see com.mongodb.client.MongoIterable
 * @see com.mongodb.ReadPreference
 * @see org.bson.codecs.Codec
 */
@SpringBootTest
public class MongoClientLesson extends AbstractLesson {

  private MongoClient mongoClient;

  private MongoDatabase database;

  private MongoCollection<Document> collection;

  private String uri = "mongodb+srv://m220student:m220password@mflix-gidyp.mongodb.net/test";

  private Document document;

  private Bson bson;

  @Test
  public void MongoClientInstance() {

    mongoClient = MongoClients.create(uri);

    Assert.assertNotNull(mongoClient);

    MongoClientSettings settings;

    ConnectionString connectionString = new ConnectionString(uri);
    MongoClientSettings clientSettings =
        MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .applicationName("mflix")
            .applyToConnectionPoolSettings(
                builder -> builder.maxWaitTime(1000, TimeUnit.MILLISECONDS))
            .build();

    mongoClient = MongoClients.create(clientSettings);

    Assert.assertNotNull(mongoClient);
  }

  @Test
  public void MongoDatabaseInstance() {

    mongoClient = MongoClients.create(uri);

    MongoIterable<String> databaseIterable = mongoClient.listDatabaseNames();

    List<String> dbnames = new ArrayList<>();
    for (String name : databaseIterable) {
      System.out.println(name);
      dbnames.add(name);
    }

    Assert.assertTrue(dbnames.contains("mflix"));


    database = mongoClient.getDatabase("mflix");
    ReadPreference readPreference = database.getReadPreference();
    Assert.assertEquals("primary", readPreference.getName());
  }

  @Test
  public void MongoCollectionInstance() {

    mongoClient = MongoClients.create(uri);
    database = mongoClient.getDatabase("mflix");
    collection = database.getCollection("movies");

    MongoIterable<Document> cursor = collection.find().skip(10).limit(20);

    List<Document> documents = new ArrayList<>();
    Assert.assertEquals(20, cursor.into(documents).size());
  }

  @Test
  public void DocumentInstance() {
    mongoClient = MongoClients.create(uri);
    database = mongoClient.getDatabase("test");
    collection = database.getCollection("users");

    document = new Document("name", new Document("first", "Norberto").append("last", "Leite"));

    collection.insertOne(document);

    Assert.assertTrue(document instanceof Bson);


  }
}
