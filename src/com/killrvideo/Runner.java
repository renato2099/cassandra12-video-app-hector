package com.killrvideo;


import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class Runner {


	// CHANGE ME!! If you have a different cluster name, this is the place to change it.
	private static final String CLUSTER_NAME = "Test Cluster";
	
	// If you used the database setup file with this project, the default should be fine. Change if you need to.
	private static final String KEYSPACE = "Killrvideo";

    /**
     * @param args
     */
    public static void main(String[] args) {

        System.out.println(UUID.randomUUID().toString());

        UUID videoId = UUID.randomUUID();

        Cluster myCluster = HFactory.getOrCreateCluster(CLUSTER_NAME, "localhost:9160");
        Keyspace keyspace = HFactory.createKeyspace(KEYSPACE, myCluster);

        //settingData(videoId, keyspace);
        retrievingData(videoId, keyspace);

    }

    private static void retrievingData(UUID videoId, Keyspace keyspace){
        User user = new User("pmcfadin", "secretPassword123", "Patrick", "McFadin");
        BusinessLogic bl = new BusinessLogic();
        //bl.getUserAll(user.getUsername(), keyspace);
        System.out.println("User: ");
        printData(bl.getUserAll(user.getUsername(), keyspace));

        System.out.println("Videos: ");
        ArrayList<Video> aVideos = bl.getVideos(1, keyspace);
        printData(aVideos);

        if(aVideos.size() >= 1) {
            System.out.println("Comments: ");
            printData(bl.getComments(aVideos.get(0).getVideoId(), keyspace));

            System.out.println("Video rating: ");
            printData(bl.getRating(aVideos.get(0).getVideoId(), keyspace));
            
            System.out.println("Comments by time slice: ");
            
            System.out.println();
            printData(bl.getCommentsOnTimeSlice(getTimeStampOffset("2013-01-17 13:31:05.072", -1),
                                                getTimeStampOffset("2013-01-17 13:31:05.072", 1),
                                                aVideos.get(0).getVideoId(),
                                                keyspace));
            
            System.out.println("Video with latest timestamp");
            printData(bl.getVideoLastStopEvent(aVideos.get(0).getVideoId(), "pmcfadin", keyspace));
        }
    }

    private static Timestamp getTimeStampOffset(String pTimeStamp, int pOffset){
        // 2013-01-13 18:13:40.791
        Timestamp commentTimestamp = null;
        SimpleDateFormat datetimeFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        try {
            
            Date lFromDate1 = datetimeFormatter1.parse(pTimeStamp);
            commentTimestamp = new Timestamp(lFromDate1.getTime());
            Calendar c = Calendar.getInstance();
            c.setTime(lFromDate1);
            c.add(Calendar.MINUTE, pOffset);
            Long time = c.getTime().getTime();
            commentTimestamp = new Timestamp(time);
            
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return commentTimestamp;
    }
    
    private static Timestamp getTimeStampOffset(int pOffset){
        long time = 0;
        Date curDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar c = Calendar.getInstance();
        c.setTime(curDate);
        c.add(Calendar.MINUTE, pOffset);  // number of days to add
        time = c.getTime().getTime();
        return new Timestamp(time);
    }
	private static void settingData(UUID videoId, Keyspace keyspace){
		// Create a user and video for testing
		User user = new User("pmcfadin", "secretPassword123", "Patrick", "McFadin");
		Video video = new Video(videoId, "Funny Cat Video", "pmcfadin", "A video about a cat. It's pretty funny.", new String[]{"Cats","Funny","lol"});
				
		BusinessLogic bl = new BusinessLogic();
				
		System.out.println("Setting a user");
		//bl.setUser(user, keyspace);

		System.out.println("Setting a video");
		bl.setVideo(video, keyspace);

		System.out.println("Setting a video with tag index");
		bl.setVideoWithTagIndex(video, keyspace);

		System.out.println("Getting video by UUID");
		bl.getVideoByUUID(videoId, keyspace);
		
		System.out.println("Setting a comment for a video");
		bl.setComment(video, "Kinda meh. I like southpark better", new Timestamp(new java.util.Date().getTime()), keyspace);

		System.out.println("Rating a video");
		bl.setRating(videoId, 4, keyspace);

		System.out.println("Setting a start event");
		Timestamp startEvent = new Timestamp(new java.util.Date().getTime());
		bl.setVideoStartEvent(videoId, "pmcfadin", startEvent, keyspace);

		System.out.println("Setting a stop event");
		Timestamp stopEvent = new Timestamp(new java.util.Date().getTime());
		Timestamp videoTimestamp = new Timestamp(new java.util.Date().getTime());
		bl.setVideoStopEvent(videoId, "pmcfadin", stopEvent, videoTimestamp, keyspace);
	}

    private static void printData(Object pObj){
        if(pObj != null)
            System.out.println(pObj.toString());
        else
            System.out.println("Object null");
    }

    private static void printData(ArrayList pArrayList){
        for(Object pObj : pArrayList){
            System.out.println(pObj.toString());
        }
    }
}