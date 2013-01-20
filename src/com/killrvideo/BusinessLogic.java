package com.killrvideo;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

public class BusinessLogic {

    public static final String COMPOSITE_KEY = "ALL";
    public static final String EVENT_TIMESTAMP = "video_event";
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    private static UUIDSerializer uuidSerializer = UUIDSerializer.get();
    private static TimeUUIDSerializer timeuuidSerializer = TimeUUIDSerializer.get();

    public void setUser(User user, Keyspace keyspace) {

        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

        try {

            mutator.addInsertion(user.getUsername(), "users",
                    HFactory.createStringColumn("firstname", user.getFirstname()));
            mutator.addInsertion(user.getUsername(), "users",
                    HFactory.createStringColumn("lastname", user.getLastname()));
            mutator.addInsertion(user.getUsername(), "users",
                    HFactory.createStringColumn("password", user.getPassword()));

            mutator.execute();
         } catch (HectorException he) {
           throw he;
         }
    }

    public User getUserAll(String username, Keyspace keyspace){
        UserIterator ti = new UserIterator(username, keyspace);
        
        User user = new User();
        user.setUsername(username);
        
        int count = 0;
        for (HColumn<String,String> column : ti ) {
              if(column.getName().equals("firstname")) user.setFirstname(column.getValue());
              if(column.getName().equals("lastname")) user.setLastname(column.getValue());
              if(column.getName().equals("password")) user.setPassword(column.getValue());
              count++;
        }
        
        System.out.println("Read a total of " + count + "columns");
        return user;
    }
    
    public User getUser(String username, Keyspace keyspace) {
        User user = new User();

        // Create a slice query. We'll be getting specific column names
        SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer,
                stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("users");
        sliceQuery.setKey(username);
        sliceQuery.setColumnNames("lastname", "firstname", "password");
        
        // Execute the query and get the list of columns
        ColumnSlice<String, String> result = sliceQuery.execute().get();

        // Get each column by name and add them to our video object
        user.setUsername(username);
        user.setLastname(result.getColumnByName("lastname").getValue());
        user.setFirstname(result.getColumnByName("firstname").getValue());
        user.setPassword(result.getColumnByName("password").getValue());

        return user;
    }

    public void setVideo(Video video, Keyspace keyspace) {

        Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());

        try {
            mutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("videoname", video.getVideoName()));
            mutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("username", video.getUsername()));
            mutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("description", video.getDescription()));
            mutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("tags", video.getDelimitedTags()));

            mutator.execute();
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

    public Video getVideoByUUID(UUID videoId, Keyspace keyspace) {

        Video video = new Video();

        // Create a slice query. We'll be getting specific column names
        SliceQuery<UUID, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, uuidSerializer,
                stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("videos");
        sliceQuery.setKey(videoId);

        sliceQuery.setColumnNames("videoname", "username", "description", "tags");

        // Execute the query and get the list of columns
        ColumnSlice<String, String> result = sliceQuery.execute().get();

        // Get each column by name and add them to our video object
        video.setVideoName(result.getColumnByName("videoname").getValue());
        video.setUsername(result.getColumnByName("username").getValue());
        video.setDescription(result.getColumnByName("description").getValue());
        video.setTags(result.getColumnByName("tags").getValue().split(","));

        return video;
    }

    public ArrayList<Video> getVideos(int pCount, Keyspace keyspace){
        ArrayList<Video> videos = new ArrayList<Video> ();
        RangeSlicesQuery<UUID, String, String> rangeSlicesQuery = HFactory
                .createRangeSlicesQuery(keyspace, uuidSerializer, stringSerializer, stringSerializer)
                .setColumnFamily("videos")
                .setRowCount(pCount)
                .setRange(null, null, false, 10);// -> only ten columns

        UUID last_key = null;

        while(true){
            rangeSlicesQuery.setKeys(last_key, null);
            Video video = new Video();

            QueryResult<OrderedRows<UUID, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<UUID, String, String> rows = result.get();
            Iterator<Row<UUID, String, String>> rowsIterator = rows.iterator();

            // we'll skip this first one, since it is the same as the last one from previous time we executed
            // if (last_key != null && rowsIterator != null) rowsIterator.next();   

            while (rowsIterator.hasNext()) {
              Row<UUID, String, String> row = rowsIterator.next();
              last_key = row.getKey();

              // checking if the row is not empty
              if (row.getColumnSlice().getColumns().isEmpty()) {
                continue;
              }

              video.setVideoId(row.getKey());
              video.setVideoName(row.getColumnSlice().getColumnByName("videoname").getValue());
              video.setDescription(row.getColumnSlice().getColumnByName("description").getValue());
              video.setUsername(row.getColumnSlice().getColumnByName("username").getValue());
              video.setTags(row.getColumnSlice().getColumnByName("tags").getValue().split(","));
              videos.add(video);

            }
            System.out.println(rows.getCount());
            if (rows.getCount() <= pCount)
                break;
        }
        
        return videos;
    }

    public void setVideoWithTagIndex(Video video, Keyspace keyspace) {

        Mutator<UUID> UUIDmutator = HFactory.createMutator(keyspace, UUIDSerializer.get());
        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

        try {

            UUIDmutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("videoname", video.getVideoName()));
            UUIDmutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("username", video.getUsername()));
            UUIDmutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("description", video.getDescription()));
            UUIDmutator.addInsertion(video.getVideoId(), "videos",
                    HFactory.createStringColumn("tags", video.getDelimitedTags()));

            for (String tag : video.getTags()) {
                mutator.addInsertion(tag, "tag_index", HFactory.createStringColumn(video.getVideoId().toString(), ""));
            }

            UUIDmutator.execute();
            mutator.execute();

        } catch (HectorException he) {
            he.printStackTrace();
        }

    }

    public void setVideoWithUserIndex(Video video, Keyspace keyspace) {
        // TODO Implement this method
        /*
         * This mthod is similar to the setVideo but with a subtle twist. When
         * you insert a new video, you will need to insert into the
         * username_video_index at the same time for username to video lookups.
         */

    }

    public void setComment(Video video, String comment, Timestamp timestamp, Keyspace keyspace) {

        Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());

        try {
            String columnName = video.getUsername() + ":" + timestamp;
            mutator.addInsertion(video.getVideoId(), "comments", HFactory.createStringColumn(columnName, comment));

            mutator.execute();
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

    public ArrayList<String> getComments(UUID videoId, Keyspace keyspace) {
        CommentIterator ti = new CommentIterator(videoId, keyspace);
        ArrayList<String> comments = new ArrayList<String>();
        int count = 0;
        for (HColumn<UUID,String> column : ti ) {
            System.out.println(column.getName() + ": "+  column.getValue());
            count++;
        }
        System.out.println("Read a total of " + count + "columns");
        return comments;
    }

    public ArrayList<String> getCommentsOnTimeSlice(Timestamp startTimestamp, Timestamp stopTimestamp, UUID videoId, Keyspace keyspace) {
        // TODO Implement
        /*
         * Each video can have a unbounded list of comments associated with it.
         * This method should return comments from one timestamp to another.
         */
        ArrayList<String> comments = new ArrayList<String> ();
        SimpleDateFormat datetimeFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
                .createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer)
                .setColumnFamily("comments")
                //.setRowCount(pCount)
                .setRange(null, null, false, Integer.MAX_VALUE);// -> only ten columns

        String last_key = null;
        rangeSlicesQuery.setKeys(last_key, null);

        QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
        OrderedRows<String, String, String> rows = result.get();
        Iterator<Row<String, String, String>> rowsIterator = rows.iterator();
        
        while (rowsIterator.hasNext()) {
            Row<String, String, String> row = rowsIterator.next();
            last_key = row.getKey();

            // checking if the row is not empty
            if (row.getColumnSlice().getColumns().isEmpty()) {
                continue;
            }
            Iterator<HColumn<String, String>> it = row.getColumnSlice().getColumns().iterator();
            // checking the columns
            while(it.hasNext()){
                HColumn<String, String> tmpCol = it.next();
                int iPosTime = tmpCol.getName().indexOf(":");
                if (iPosTime > 0){
                    try {
                        String strComTimestamp = tmpCol.getName().substring(iPosTime+1, tmpCol.getName().length());
                        String commentUser = tmpCol.getName().substring(0,iPosTime);
                        Date lFromDate1 = datetimeFormatter1.parse(strComTimestamp);
                        Timestamp commentTimestamp = new Timestamp(lFromDate1.getTime());
                        if(commentTimestamp.after(startTimestamp) && commentTimestamp.before(stopTimestamp))
                            comments.add(commentUser + tmpCol.getValue());
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                }
            }
        }
        return comments;
    }

    public void setRating(UUID videoId, long ratingNumber, Keyspace keyspace) {
        Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUIDSerializer.get());

        try {
            mutator.addCounter(videoId, "video_rating", HFactory.createCounterColumn("rating_count", 1));
            mutator.addCounter(videoId, "video_rating", HFactory.createCounterColumn("rating_total", ratingNumber));
            mutator.execute();
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

    public float getRating(UUID videoId, Keyspace keyspace) {
        // TODO Implement
// http://stackoverflow.com/questions/7968396/how-can-i-get-a-value-of-a-countercolumn-out-of-cassandra-with-hector
        /*
         * Each video has two things. a rating_count and rating_total. The
         * average rating is calculated by taking the total and dividing by the
         * count. Build the logic to get both numbers and return the average.
         */
        // Create a slice query. We'll be getting specific column names
        
        CounterQuery<UUID,String> counterQuery = HFactory.createCounterColumnQuery(keyspace, uuidSerializer,stringSerializer);
        counterQuery.setColumnFamily("video_rating");
        counterQuery.setName("rating_count");
        counterQuery.setKey(videoId);
        
        HCounterColumn<String> queryResult = counterQuery.execute().get();
        long ratingCount = queryResult.getValue();
        counterQuery.setName("rating_total");
        long ratingTotal = counterQuery.execute().get().getValue();
        //System.out.println(ratingCount);
        //System.out.println(ratingTotal);
        return (float) ((ratingCount + ratingTotal) / 2.0);
    }

    public void setVideoStartEvent(UUID videoId, String username, Timestamp timestamp, Keyspace keyspace) {
        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        try {
            //mutator.addInsertion(username + ":" + videoId, "video_event",
            //        HFactory.createStringColumn("start:" + timestamp, ""));
            Composite composite = new Composite();
            composite.setComponent(0,"start", StringSerializer.get());
            //composite.setComponent(1,timestamp.toString(), stringSerializer);
            composite.setComponent(1, TimeUUIDUtils.getTimeUUID(timestamp.getTime()), uuidSerializer);
            HColumn<Composite,String> col = HFactory.createColumn(composite, "", new CompositeSerializer(), stringSerializer);
            mutator.addInsertion(username + ":" + videoId, EVENT_TIMESTAMP, col);
            
            mutator.execute();
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

    public void setVideoStopEvent(UUID videoId, String username, Timestamp stopEvent, Timestamp videoTimestamp,
            Keyspace keyspace) {
        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        try {
            //mutator.addInsertion(username + ":" + videoId, "video_event",
            //        HFactory.createStringColumn("stop:" + stopEvent, videoTimestamp.toString()));
            
            Composite composite = new Composite();
            composite.addComponent("stop", StringSerializer.get());
            composite.addComponent(TimeUUIDUtils.getTimeUUID(stopEvent.getTime()), uuidSerializer);
            HColumn<Composite,String> col = HFactory.createColumn(composite,
                                                                    videoTimestamp.toString(),
                                                                    new CompositeSerializer(),
                                                                    stringSerializer);
            mutator.addInsertion(username + ":" + videoId, EVENT_TIMESTAMP, col);
            
            mutator.execute();
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

    public Timestamp getVideoLastStopEvent(UUID videoId, String username, Keyspace keyspace) {
        /*
         * This method will return the video timestamp of the last stop event
         * for a given video identified by videoid. As a hint, you will be using
         * a getSlice to find certain strings to narrow the search.
         */
        Timestamp ts = null; 
        String startArg = "stop";

        // Note the use of 'equal' and 'greater-than-equal' for the start and end.
        // this has to be the case when we want all 
        Composite start = compositeFrom(startArg, Composite.ComponentEquality.EQUAL);
        Composite end = compositeFrom(startArg, Composite.ComponentEquality.GREATER_THAN_EQUAL);

        VideoEventCompositeQueryIterator iter =
                                                new VideoEventCompositeQueryIterator(
                                                      username + ":" + videoId, 
                                                      start, 
                                                      end, 
                                                      keyspace);

        System.out.printf("Printing all columns starting with \"%s\"\n", startArg);
        int count = 0;
        for ( HColumn<Composite,String> column : iter ) {

          System.out.printf("Country code: %s  Admin Code: %s  Name: %s  Timezone: %s \n",
            column.getName().get(0,StringSerializer.get()),
            column.getName().get(1,StringSerializer.get()),
            column.getName().get(2,StringSerializer.get()),
            column.getValue()
            );
          count++;
        }
        System.out.printf("Found %d columns\n",count);
        
        return ts;
    }

    /**
     * Encapsulates the creation of Composite to make it easier to experiment with values
     * 
     * @param componentName
     * @param equalityOp
     * @return
     */
    public static Composite compositeFrom(String componentName, Composite.ComponentEquality equalityOp) {
        Composite composite = new Composite();
        composite.setEquality(equalityOp);
        composite.addComponent(0, componentName, equalityOp);
        return composite;
      }
    
    /**
     * Demonstrates the use of Hector's ColumnSliceIterator for "paging" automatically over the results
     *
     */
    class VideoEventCompositeQueryIterator implements Iterable<HColumn<Composite,String>> {

      private final String key;
      private final ColumnSliceIterator<String,Composite,String> sliceIterator;
      private Composite start;
      private Composite end;

      VideoEventCompositeQueryIterator(String key, Composite start, Composite end, Keyspace keyspace) {
        this.key = key;
        this.start = start;
        this.end = end;

        SliceQuery<String,Composite,String> sliceQuery =
          HFactory.createSliceQuery(keyspace, stringSerializer, 
                                    new CompositeSerializer(),
                                    StringSerializer.get());
        sliceQuery.setColumnFamily("video_event");
        sliceQuery.setKey(key);

        sliceIterator = new ColumnSliceIterator<String, Composite, String>(
                                              sliceQuery,
                                              this.start,
                                              this.end,
                                              false);

      }

      public Iterator<HColumn<Composite, String>> iterator() {
        return sliceIterator;
      }
    }
    
   /**
    * An iterator implementation is a clean way to pass back up to the caller
    * the concept of seemlessly scanning a wide row in an efficient manner.
    */
    class UserIterator implements Iterable<HColumn<String,String>> {
        private ColumnSliceIterator<String,String,String> sliceIterator;

        UserIterator(String key, Keyspace keyspace) {
        SliceQuery<String,String,String> sliceQuery =
                HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
        sliceQuery.setColumnFamily("users");
        sliceQuery.setKey(key);
        sliceIterator = new ColumnSliceIterator<String,String,String>(sliceQuery,"","",false);
        }

        @Override
        public Iterator<HColumn<String, String>> iterator() {
            return sliceIterator;
        }
      }

     /**
      * An iterator implementation is a clean way to pass back up to the caller
      * the concept of seemlessly scanning a wide row in an efficient manner.
      */
      class CommentIterator implements Iterable<HColumn<UUID,String>> {
        private ColumnSliceIterator<UUID,String,String> sliceIterator;

        CommentIterator(UUID key, Keyspace keyspace) {
            SliceQuery<UUID,String,String> sliceQuery =
                    HFactory.createSliceQuery(keyspace, uuidSerializer, stringSerializer, stringSerializer);
            sliceQuery.setColumnFamily("comments");
            sliceQuery.setKey(key);
            sliceIterator = new ColumnSliceIterator<UUID,String,String>(sliceQuery,"","",false);
        }
        
        @Override
        public Iterator<HColumn<UUID, String>> iterator() {
            return sliceIterator;
        }
      }

      /**
       * An iterator implementation is a clean way to pass back up to the caller
       * the concept of seemlessly scanning a wide row in an efficient manner.
       */
      class VideoEventIterator implements Iterable<HColumn<String, String>>{
        private ColumnSliceIterator<String,String,String> sliceIterator;
        
        VideoEventIterator(String key, Keyspace keyspace) {
            SliceQuery<String,String,String> sliceQuery =
                    HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
            sliceQuery.setColumnFamily("video_event");
            sliceQuery.setKey(key);
            sliceIterator = new ColumnSliceIterator<String,String,String>(sliceQuery,"","",false);
        }
        
        @Override
        public Iterator<HColumn<String, String>> iterator() {
            // TODO Auto-generated method stub
            return sliceIterator;
        }
          
      }
}
