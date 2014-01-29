package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "unnamed.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		
		// Scrollable result set allows us to read forward (using next())
		// and also backward.  
		// This is needed here to support the user of isFirst() and isLast() methods,
		// but in many cases you will not need it.
		// To create a "normal" (unscrollable) statement, you would simply call
		// Statement stmt = oracleConnection.createStatement();
		//
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		
		// For each month, find the number of friends born that month
		// Sort them in descending order of count
		ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from "+
				userTableName+
				" where month_of_birth is not null group by month_of_birth order by 1 desc");
		
		this.monthOfMostFriend = 0;
		this.monthOfLeastFriend = 0;
		this.totalFriendsWithMonthOfBirth = 0;
		
		// Get the month with most friends, and the month with least friends.
		// (Notice that this only considers months for which the number of friends is > 0)
		// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
		//
		while(rst.next()) {
			int count = rst.getInt(1);
			int month = rst.getInt(2);
			if (rst.isFirst())
				this.monthOfMostFriend = month;
			if (rst.isLast())
				this.monthOfLeastFriend = month;
			this.totalFriendsWithMonthOfBirth += count;
		}
		
		// Get the names of friends born in the "most" month
		rst = stmt.executeQuery("select user_id, first_name, last_name from "+
				userTableName+" where month_of_birth="+this.monthOfMostFriend);
		while(rst.next()) {
			Long uid = rst.getLong(1);
			String firstName = rst.getString(2);
			String lastName = rst.getString(3);
			this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Get the names of friends born in the "least" month
		rst = stmt.executeQuery("select first_name, last_name, user_id from "+
				userTableName+" where month_of_birth="+this.monthOfLeastFriend);
		while(rst.next()){
			String firstName = rst.getString(1);
			String lastName = rst.getString(2);
			Long uid = rst.getLong(3);
			this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							      ResultSet.CONCUR_READ_ONLY);
	    ResultSet rst = stmt.executeQuery("SELECT last_name, LENGTH(last_name) " +
					      "FROM " + userTableName + " " +
					      "ORDER BY 2 DESC");

	    int longest_length, shortest_length;
	    if(rst.next()) {
		longest_length = rst.getInt(2);
		this.longestLastNames.add(rst.getString(1));
		while(rst.next()) {
		    if(rst.getInt(2) == longest_length) this.longestLastNames.add(rst.getString(1));
		    else break;
		}
		
		rst.last();
		shortest_length = rst.getInt(2);
		this.shortestLastNames.add(rst.getString(1));
		while(rst.previous()) {
		    if(rst.getInt(2) == shortest_length) this.shortestLastNames.add(rst.getString(1));
		    else break;
		}
	    }


	    rst = stmt.executeQuery("SELECT COUNT (*), last_name " +
				    "FROM " + userTableName + " " +
				    "WHERE last_name IS NOT NULL " +
				    "GROUP BY last_name " +
				    "ORDER BY 1 DESC");

	    if(rst.next()) {
		mostCommonLastNamesCount = rst.getInt(1);
		this.mostCommonLastNames.add(rst.getString(2));
		while(rst.next()) {
		    if(rst.getInt(1) == mostCommonLastNamesCount)
			mostCommonLastNames.add(rst.getString(2));
		    else break;
		}
	    }


	    // Close statement and result set
	    rst.close();
	    stmt.close();

	}











	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have no friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void lonelyFriends() throws SQLException {
	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							      ResultSet.CONCUR_READ_ONLY);
	    ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.first_name, U.last_name " +
					      "FROM " + userTableName + " U " +
					      "WHERE U.user_id NOT IN (SELECT F.user1_id " +
					                              "FROM " + friendsTableName + " F " +
					                              "UNION " +
					                              "SELECT F.user2_id " +
					                              "FROM " + friendsTableName + " F)"
					      );
	    this.countLonelyFriends = 0;
	    while(rst.next()) {
		this.countLonelyFriends++;
		this.lonelyFriends.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
	    }


	    // Close statement and result set
	    rst.close();
	    stmt.close();
	}












	 

	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							      ResultSet.CONCUR_READ_ONLY);

	    ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.first_name, U.last_name " +
					      "FROM " + userTableName + " U, " +
					                currentCityTableName + " C, " +
					                hometownCityTableName + " H " +
					      "WHERE U.user_id = C.user_id " +
					      "AND U.user_id = H.user_id " +
					      "AND C.current_city_id = H.hometown_city_id"
					      );
	    this.countLiveAtHome = 0;
	    while(rst.next()) {
		this.countLiveAtHome++;
		this.liveAtHome.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
	    }

	    // Close statement and result set
	    rst.close();
	    stmt.close();
	}



















	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							      ResultSet.CONCUR_READ_ONLY);

	    ResultSet rst = stmt.executeQuery("SELECT PC.cnt, PC.photo_id, PC.album_id, PC.album_name, PC.photo_caption, PC.photo_link, " +
					             "U.user_id, U.first_name, U.last_name " +
					      "FROM " + userTableName + " U, " + tagTableName + " T, " +
					          "(SELECT COUNT (*) AS cnt, P.photo_id, A.album_id, A.album_name, P.photo_caption, P.photo_link " +
					           "FROM " + photoTableName + " P, " + albumTableName + " A, " + tagTableName + " T " +
					           "WHERE P.photo_id = T.tag_photo_id " +
					           "AND P.album_id = A.album_id " +
					           "GROUP BY P.photo_id, A.album_id, A.album_name, P.photo_caption, P.photo_link" +
					          ") PC " +
					      "WHERE T.tag_photo_id = PC.photo_id " +
					      "AND T.tag_subject_id = U.user_id " +
					      "ORDER BY cnt DESC, PC.photo_id"
					      );

	    if(rst.next()) {
		boolean more = true;
		for(int i=0; i<n; i++) {
		    String photoId = rst.getString("photo_id");
		    String albumId = rst.getString("album_id");
		    String albumName = rst.getString("album_name");
		    String photoCaption = rst.getString("photo_caption");
		    String photoLink = rst.getString("photo_link");
		    PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
		    TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

		    for(int j=0; j<rst.getInt("cnt"); j++) {
			tp.addTaggedUser(new UserInfo(rst.getLong("user_id"), rst.getString("first_name"), rst.getString("last_name")));
			if(!rst.next()) {
			    more = false;
			    break;
			}
		    }
		    this.photosWithMostTags.add(tp);
		    if(!more)
			break;
		}
	    }


	    // Close statement and result set
	    rst.close();
	    stmt.close();

	}

	








	
	
	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
							      ResultSet.CONCUR_READ_ONLY);

	    ResultSet rst = stmt.executeQuery("SELECT PF.cnt, PF.u1_id, PF.u1_fn, PF.u1_ln, PF.u1_yob, PF.u2_id, PF.u2_fn, PF.u2_ln, PF.u2_yob, " +
	    					             "PT.photo_id, PT.photo_caption, PT.photo_link, " +
	    					             "A.album_id, A.album_name " +
					      "FROM " + photoTableName + " PT, " + albumTableName + " A, " + tagTableName + " TG1, " + tagTableName + " TG2, " +
					          "(SELECT COUNT (*) AS cnt, U1.user_id AS u1_id, U1.first_name AS u1_fn, U1.last_name AS u1_ln, U1.year_of_birth AS u1_yob, " +
					                                    "U2.user_id AS u2_id, U2.first_name AS u2_fn, U2.last_name AS u2_ln, U2.year_of_birth AS u2_yob " +
					             "FROM " + userTableName + " U1, " + userTableName + " U2, " +
					                       photoTableName + " P, " + 
					                       tagTableName + " T1, " + tagTableName + " T2 " +
					             "WHERE U1.gender='female' AND U2.gender='male' " +
					             "AND ABS(U1.year_of_birth - U2.year_of_birth) <= " + yearDiff + " " +
					             "AND NOT EXISTS (SELECT 1 " +
					                             "FROM " + friendsTableName + " F " +
					                             "WHERE F.user1_id=U1.user_id AND F.user2_id=U2.user_id) " +
					             "AND NOT EXISTS (SELECT 1 " +
					                             "FROM " + friendsTableName + " F " +
					                             "WHERE F.user1_id=U2.user_id AND F.user2_id=U1.user_id) " +
					             "AND P.photo_id = T1.tag_photo_id " +
					             "AND P.photo_id = T2.tag_photo_id " +
					             "AND T1.tag_subject_id = U1.user_id " +
					             "AND T2.tag_subject_id = U2.user_id " +
					             "GROUP BY U1.user_id, U1.first_name, U1.last_name, U1.year_of_birth, U2.user_id, U2.first_name, U2.last_name, U2.year_of_birth" +
					            ") PF " +
					      "WHERE PT.photo_id = TG1.tag_photo_id " +
					      "AND PT.photo_id = TG2.tag_photo_id " +
					      "AND TG1.tag_subject_id = PF.u1_id " +
					      "AND TG2.tag_subject_id = PF.u2_id " +
					      "AND PT.album_id = A.album_id " +
					      "ORDER BY PF.cnt DESC, PF.u1_id, PF.u2_id"
					      );

	    if(rst.next()) {
		boolean more = true;
		for(int i=0; i<n; i++) {
		    Long girlUserId = rst.getLong("u1_id");
		    String girlFirstName = rst.getString("u1_fn");
		    String girlLastName = rst.getString("u1_ln");
		    int girlYear = rst.getInt("u1_yob");
		    Long boyUserId = rst.getLong("u2_id");
		    String boyFirstName = rst.getString("u2_fn");
		    String boyLastName = rst.getString("u2_ln");
		    int boyYear = rst.getInt("u2_yob");
		    MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
						 girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		    for(int j=0; j<rst.getInt("cnt"); j++) {
			String sharedPhotoId = rst.getString("photo_id");
			String sharedPhotoAlbumId = rst.getString("album_id");
			String sharedPhotoAlbumName = rst.getString("album_name");
			String sharedPhotoCaption = rst.getString("photo_caption");
			String sharedPhotoLink = rst.getString("photo_link");
			mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
							sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
			if(!rst.next()) {
			    more = false;
			    break;
			}
		    }
		    this.bestMatches.add(mp);
		    if(!more)
			break;
		}
	    }


	    // Close statement and result set
	    rst.close();
	    stmt.close();
	}















	
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {

	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

	    ResultSet rst = stmt.executeQuery("SELECT PR.cnt, PR.u1_id, PR.u1_fn, PR.u1_ln, PR.u2_id, PR.u2_fn, PR.u2_ln, U.user_id, U.first_name, U.last_name " +
					      "FROM " + userTableName + " U, " + friendsTableName + " FR1, " + friendsTableName + " FR2, " +
				      
					              "(SELECT COUNT (*) AS cnt, U1.user_id AS u1_id, U1.first_name AS u1_fn, U1.last_name AS u1_ln, " +
					                                       "U2.user_id AS u2_id, U2.first_name AS u2_fn, U2.last_name AS u2_ln " +
					              "FROM " + userTableName + " U1, " + userTableName + " U2, " + userTableName + " U3, " +
					                        friendsTableName + " F1, " + friendsTableName + " F2, " +

					                      "(SELECT UU1.user_id AS uu1_id, UU2.user_id AS uu2_id " + 
					                      "FROM " + userTableName + " UU1, " + userTableName + " UU2 " +
					                      "WHERE UU1.user_id < UU2.user_id " +
					                      "AND NOT EXISTS (SELECT 1 " +
					                                      "FROM " + friendsTableName + " FF " +
					                                      "WHERE FF.user1_id=UU1.user_id AND FF.user2_id=UU2.user_id)" +
					                      ") NF " +


					              "WHERE U1.user_id=NF.uu1_id AND U2.user_id=NF.uu2_id " +
					              "AND ((F1.user1_id=U1.user_id AND F1.user2_id=U3.user_id) OR (F1.user1_id=U3.user_id AND F1.user2_id=U1.user_id)) " +
					              "AND ((F2.user1_id=U2.user_id AND F2.user2_id=U3.user_id) OR (F2.user1_id=U3.user_id AND F2.user2_id=U2.user_id)) " +
					              "GROUP BY U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name " +
					              "HAVING COUNT (*) > 10" +
					             ") PR " +
					      "WHERE ((FR1.user1_id=U.user_id AND FR1.user2_id=PR.u1_id) OR (FR1.user1_id=PR.u1_id AND FR1.user2_id=U.user_id)) " +
					      "AND ((FR2.user1_id=U.user_id AND FR2.user2_id=PR.u2_id) OR (FR2.user1_id=PR.u2_id AND FR2.user2_id=U.user_id)) " +
					      "ORDER BY PR.cnt DESC, PR.u1_id, PR.u2_id"
					      );

	    if(rst.next()) {
		boolean more = true;
		for(int i=0; i<n; i++) {
		    Long user1_id = rst.getLong("u1_id");
		    String user1FirstName = rst.getString("u1_fn");
		    String user1LastName = rst.getString("u1_ln");
		    Long user2_id = rst.getLong("u2_id");
		    String user2FirstName = rst.getString("u2_fn");
		    String user2LastName = rst.getString("u2_ln");
		    FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

		    for(int j=0; j<rst.getInt("cnt"); j++) {
			p.addSharedFriend(rst.getLong("user_id"), rst.getString("first_name"), rst.getString("last_name"));
			if(!rst.next()) {
			    more = false;
			    break;
			}
		    }
		    this.suggestedFriendsPairs.add(p);
		    if(!more)
			break;
		}
	    }



	    // Close statement and result set
	    rst.close();
	    stmt.close();

	}
	




















	
	//@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {

	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	    ResultSet rst = stmt.executeQuery("SELECT U.user_id, U.first_name, U.last_name " +
					      "FROM " + friendsTableName + " F, " + userTableName + " U " +
					      "WHERE (F.user1_id = " + user_id + " AND U.user_id = F.user2_id) OR (F.user2_id = " + user_id + " AND U.user_id = F.user1_id) " +
					      "ORDER BY U.year_of_birth ASC, U.month_of_birth ASC, U.day_of_birth ASC, U.user_id DESC"
					      );

	    if(rst.next()) {
		Long uid = rst.getLong(1);
		String firstName = rst.getString(2);
		String lastName = rst.getString(3);
		this.oldestFriend = new UserInfo(uid, firstName, lastName);
	    }
	    if(rst.last()) {
		Long uid = rst.getLong(1);
		String firstName = rst.getString(2);
		String lastName = rst.getString(3);
		this.youngestFriend = new UserInfo(uid, firstName, lastName);
	    }
	    rst.close();
	    stmt.close();
	}
	
	























	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {

	    Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
	    ResultSet rst = stmt.executeQuery("SELECT city_name, count(*) " +
					      "FROM "+ eventTableName + ", " + cityTableName + " " +
					      "WHERE city_id = event_city_id " +
					      "GROUP BY city_name " +
					      "HAVING count(*) = (SELECT max(count(*)) " +
					                         "FROM "+ eventTableName + " " +
					                         "GROUP BY event_city_id)");

	    this.eventCount = 0;
	    while (rst.next()){
		if (rst.isFirst())
		    this.eventCount = rst.getInt(2);
		String cityName = rst.getString(1);
		this.popularCityNames.add(cityName);
	    }
	    rst.close();
	    stmt.close();
	    


	    /*
	    this.eventCount = 12;
	    this.popularCityNames.add("Ann Arbor");
	    this.popularCityNames.add("Ypsilanti");
	    */
	}
	
	
	




















	@Override
	// ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings() throws SQLException {

	    Statement stmt = oracleConnection.createStatement();
	    ResultSet rst = stmt.executeQuery("SELECT U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name " +
					      "FROM " + userTableName + " U1, " + userTableName + " U2, " +
					      hometownCityTableName + " H1, " + hometownCityTableName + " H2, " + friendsTableName+" F " +
					      "WHERE U1.user_id < U2.user_id " +
					      "AND U1.last_name = U2.last_name " +
					      "AND U1.user_id = F.user1_id " + 
					      "AND U2.user_id = F.user2_id " +
					      "AND U1.user_id = H1.user_id " +
					      "AND U2.user_id = H2.user_id " +
					      "AND H1.hometown_city_id = H2.hometown_city_id " +
					      "AND ABS(U1.year_of_birth - U2.year_of_birth) < 10" +
					      "ORDER BY U1.user_id ASC, U2.user_id ASC");

	    while(rst.next()){
                Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
                this.siblings.add(s);
	    }
	    rst.close();
	    stmt.close();
	    
	}
	
}
