package mflix.lessons;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

@SpringBootTest
public class BasicReads extends AbstractLesson {
  public BasicReads() {
    super();
  }

  @Test
  public void testCanFindOne() {
    Document unexpected = null;

    MongoCursor cursor = moviesCollection.find(new Document()).limit(1).iterator();

    // use the next() method to get the next item in the iterator.
    Document actual = (Document) cursor.next();

    // Running this testDb, we should expect some random document, and after
    // we've consumed the iterator with next()
    Assert.assertNotEquals("should not be null", unexpected, actual);
    // we expect the iterator to have nothing left.
    Assert.assertFalse("the iterator should have no next", cursor.hasNext());
  }

  @Test
  public void testFindOneSalmaHayek() {
    Document queryFilter = new Document("cast", "Salma Hayek");

    // a shorthand findOne like used in the previous testDb
    Document actual = moviesCollection.find(queryFilter).limit(1).iterator().next();

    String expectedTitle = "Roadracers";
    int expectedYear = 1994;

    Assert.assertEquals(expectedTitle, actual.getString("title"));
    Assert.assertEquals(expectedYear, (int) actual.getInteger("year"));
  }

  @Test(expected = NoSuchElementException.class)
  public void testNoNext() {
    // Let's issue a query that won't match anything. This will throw a
    // NoSuchElementException
    moviesCollection.find(new Document("title", "foobarbizzlebazzle")).iterator().next();
  }

  @Test
  public void testTryNext() {
    Document actual =
        moviesCollection.find(new Document("title", "foobarbizzlebazzle")).iterator().tryNext();
    Assert.assertNull(actual);
  }

  @Test
  public void testFindManySalmaHayek() {
    // our query, the same as our previous "Salma Hayek" query
    Document queryFilter = new Document("cast", "Salma Hayek");

    List<Document> results = new ArrayList<>();
    moviesCollection.find(queryFilter).into(results);

    int expectedResultsSize = 29;
    Assert.assertEquals(expectedResultsSize, results.size());

    System.out.println(results.get(0).toJson());
  }

  @Test
  public void testProjection() {
    Document queryFilter = new Document("cast", "Salma Hayek");
    Document result =
        moviesCollection
            .find(queryFilter)
            .limit(1)
            .projection(new Document("title", 1).append("year", 1))
            .iterator()
            .tryNext();

    Assert.assertEquals(3, result.keySet().size());

    // And let's make sure they are what we expected
    Assert.assertTrue(result.keySet().containsAll(Arrays.asList("_id", "title", "year")));
  }

  @Test
  public void testProjectsAway_id() {

    Document queryFilter =
        new Document("cast", new Document("$all", Arrays.asList("Salma Hayek", "Johnny Depp")));

    List<Document> results = new ArrayList<>();
    moviesCollection
        .find(queryFilter)
        .projection(new Document("title", 1).append("year", 1).append("_id", 0))
        .into(results);

    // There should only be 1 result in our dataset
    Assert.assertEquals(1, results.size());

    // Now we should only have 2 keys, title and year
    Document firstResult = results.get(0);
    Assert.assertEquals(2, firstResult.keySet().size());
    Assert.assertTrue(firstResult.keySet().containsAll(Arrays.asList("title", "year")));

  }
}
