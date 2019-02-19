package mflix.lessons;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

/**
 * @see com.mongodb.client.model.Facet
 * @see com.mongodb.client.model.Accumulators
 * @see com.mongodb.client.model.Aggregates
 */
@SpringBootTest
public class UsingAggregationBuilders extends AbstractLesson {

  @Test
  public void singleStageAggregation() {

    String country = "Portugal";

    Bson countryPT = Filters.eq("countries", country);

    List<Bson> pipeline = new ArrayList<>();

    Bson matchStage = Aggregates.match(countryPT);

    // add the matchStage to the pipeline
    pipeline.add(matchStage);

    AggregateIterable<Document> iterable = moviesCollection.aggregate(pipeline);

    // collect all movies into an array list
    List<Document> builderMatchStageResults = new ArrayList<>();
    iterable.into(builderMatchStageResults);

    /*
    Which should produce a list of 152 movies produced in Portugal.
     */
    Assert.assertEquals(152, builderMatchStageResults.size());
  }

  @Test
  public void aggregateSeveralStages() {
    List<Bson> pipeline = new ArrayList<>();

    String country = "Portugal";
    Bson countryPT = Filters.eq("countries", country);
    Bson matchStage = Aggregates.match(countryPT);

    Bson unwindCastStage = Aggregates.unwind("$cast");

    String groupIdCast = "$cast";

    // use $sum accumulator to sum 1 for each cast member appearance.
    BsonField sum1 = Accumulators.sum("count", 1);

    // adding both group _id and accumulators
    Bson groupStage = Aggregates.group(groupIdCast, sum1);

     // create the sort order using Sorts builder
    Bson sortOrder = Sorts.descending("count");
    // pass the sort order to the sort stage builder
    Bson sortStage = Aggregates.sort(sortOrder);

    pipeline.add(matchStage);
    pipeline.add(unwindCastStage);
    pipeline.add(groupStage);
    pipeline.add(sortStage);

    AggregateIterable<Document> iterable = moviesCollection.aggregate(pipeline);

    List<Document> groupByResults = new ArrayList<>();
    for (Document doc : iterable) {
      System.out.println(doc);
      groupByResults.add(doc);
    }


    List<Bson> shorterPipeline = new ArrayList<>();

    shorterPipeline.add(matchStage);
    shorterPipeline.add(unwindCastStage);

    Bson sortByCount = Aggregates.sortByCount("$cast");

    shorterPipeline.add(sortByCount);

    List<Document> sortByCountResults = new ArrayList<>();

    for (Document doc : moviesCollection.aggregate(shorterPipeline)) {
      System.out.println(doc);
      sortByCountResults.add(doc);
    }

    Assert.assertEquals(groupByResults, sortByCountResults);
  }

  @Test
  public void complexStages() {

    List<Bson> pipeline = new ArrayList<>();

    // $unwind the cast array
    Bson unwindCast = Aggregates.unwind("$cast");

    // create a set of cast members with $group
    Bson groupCastSet = Aggregates.group("", Accumulators.addToSet("cast_list", "$cast"));


    Facet castMembersFacet = new Facet("cast_members", unwindCast, groupCastSet);

    // unwind genres
    Bson unwindGenres = Aggregates.unwind("$genres");

    // genres facet bucket
    Bson genresSortByCount = Aggregates.sortByCount("$genres");

    // create a genres count facet
    Facet genresCountFacet = new Facet("genres_count", unwindGenres, genresSortByCount);

    // year bucketAuto
    Bson yearBucketStage = Aggregates.bucketAuto("$year", 10);

    // year bucket facet
    Facet yearBucketFacet = new Facet("year_bucket", yearBucketStage);

      // $facets stage
    Bson facetsStage = Aggregates.facet(castMembersFacet, genresCountFacet, yearBucketFacet);

    // match stage
    Bson matchStage = Aggregates.match(Filters.eq("countries", "Portugal"));

    // putting it all together
    pipeline.add(matchStage);
    pipeline.add(facetsStage);

    int countDocs = 0;
    for (Document doc : moviesCollection.aggregate(pipeline)) {
      System.out.println(doc);
      countDocs++;
    }

    Assert.assertEquals(1, countDocs);
  }
}
