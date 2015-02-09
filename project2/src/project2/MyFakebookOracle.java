package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.sql.PreparedStatement;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "ethanjyx.";
	
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
		ResultSet rst = null; 
		PreparedStatement getMonthCountStmt = null;
		PreparedStatement getNamesMostMonthStmt = null;
		PreparedStatement getNamesLeastMonthStmt = null;
		
		try {
			// Scrollable result set allows us to read forward (using next())
			// and also backward.  
			// This is needed here to support the user of isFirst() and isLast() methods,
			// but in many cases you will not need it.
			// To create a "normal" (unscrollable) statement, you would simply call
			// stmt = oracleConnection.prepareStatement(sql);
			//
			String getMonthCountSql = "select count(*), month_of_birth from " +
				userTableName +
				" where month_of_birth is not null group by month_of_birth order by 1 desc";
			getMonthCountStmt = oracleConnection.prepareStatement(getMonthCountSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			// getMonthCountSql is the query that will run
			// For each month, find the number of friends born that month
			// Sort them in descending order of count
			// executeQuery will run the query and generate the result set
			rst = getMonthCountStmt.executeQuery();
			
			this.monthOfMostFriend = 0;
			this.monthOfLeastFriend = 0;
			this.totalFriendsWithMonthOfBirth = 0;
			while(rst.next()) {
				int count = rst.getInt(1);
				int month = rst.getInt(2);
				if (rst.isFirst())
					this.monthOfMostFriend = month;
				if (rst.isLast())
					this.monthOfLeastFriend = month;
				this.totalFriendsWithMonthOfBirth += count;
			}
			
			// Get the month with most friends, and the month with least friends.
			// (Notice that this only considers months for which the number of friends is > 0)
			// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
			//
			
			// Get the names of friends born in the "most" month
			String getNamesMostMonthSql = "select user_id, first_name, last_name from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesMostMonthStmt = oracleConnection.prepareStatement(getNamesMostMonthSql);
			
			// set the first ? in the sql above to value this.monthOfMostFriend, with Integer type
			getNamesMostMonthStmt.setInt(1, this.monthOfMostFriend);
			rst = getNamesMostMonthStmt.executeQuery();
			while(rst.next()) {
				Long uid = rst.getLong(1);
				String firstName = rst.getString(2);
				String lastName = rst.getString(3);
				this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
			}
			
			// Get the names of friends born in the "least" month
			String getNamesLeastMonthSql = "select first_name, last_name, user_id from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesLeastMonthStmt = oracleConnection.prepareStatement(getNamesLeastMonthSql);
			getNamesLeastMonthStmt.setInt(1, this.monthOfLeastFriend);
			
			rst = getNamesLeastMonthStmt.executeQuery();
			while(rst.next()){
				String firstName = rst.getString(1);
				String lastName = rst.getString(2);
				Long uid = rst.getLong(3);
				this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getMonthCountStmt != null)
				getMonthCountStmt.close();
			
			if(getNamesMostMonthStmt != null)
				getNamesMostMonthStmt.close();
			
			if(getNamesLeastMonthStmt != null)
				getNamesLeastMonthStmt.close();
		}
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
        // Find the following information from your database and store the information as shown
		/* Catherine did this query */
		ResultSet rst = null;
		PreparedStatement getNamesStmt = null;
		PreparedStatement getLongestStmt = null;
		PreparedStatement getShortestStmt = null;
		PreparedStatement getCommonStmt = null;
		Map<String, Integer> names = new TreeMap<String, Integer>();
		
		try {
			String getCommonSql = "select distinct LAST_NAME from " + userTableName;
			getCommonStmt = oracleConnection.prepareStatement(getCommonSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = getCommonStmt.executeQuery();
			while (rst.next())
				names.put(rst.getString(1), 0);
			
			String getNamesSql = "select LAST_NAME from " + userTableName +
				" order by length(LAST_NAME) DESC";
			getNamesStmt = oracleConnection.prepareStatement(getNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = getNamesStmt.executeQuery();
			
			int longest = 0;
			while(rst.next()) {
				int length = rst.getString(1).length();
				int count = names.get(rst.getString(1)).intValue();
				if (rst.isFirst()) {
					longest = length;
					this.longestLastNames.add(rst.getString(1));
				}	
				else if (length == longest)
					this.longestLastNames.add(rst.getString(1));
				names.put(rst.getString(1), count+1);
			}
			
			int shortest = 0;
			rst.setFetchDirection(ResultSet.FETCH_REVERSE);
			while(rst.previous()) {
				int length = rst.getString(1).length();
				if (rst.isLast()) {
					shortest = length;
					this.shortestLastNames.add(rst.getString(1));
				}
				else if (length == shortest)
					this.shortestLastNames.add(rst.getString(1));
			}
			
			int max = 0;
			Collection<Integer> c = names.values();
			Iterator<Integer> citr = c.iterator();
			while (citr.hasNext()) {
				int compare = citr.next().intValue();
				if (compare > max)
					max = compare;
			}
			this.mostCommonLastNamesCount = max;
			
			Set<String> s = names.keySet();
			Iterator<String> sitr = s.iterator();
			while (sitr.hasNext()) {
				if (names.get(sitr.next()).intValue() == max)
					this.mostCommonLastNames.add(sitr.next());
			}
			
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getNamesStmt != null)
				getNamesStmt.close();
			
			if(getLongestStmt != null)
				getLongestStmt.close();
			
			if(getShortestStmt != null)
				getShortestStmt.close();
			
			if(getCommonStmt != null)
				getCommonStmt.close();
		}
	}
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have strictly more than 80 friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	//Rachael did the thing
	//Rachael also doesn't know how to push things
	public void popularFriends() throws SQLException {
		// Find the following information from your database and store the information as shown 
		//this.popularFriends.add(new UserInfo(10L, "Billy", "SmellsFunny"));
		//this.popularFriends.add(new UserInfo(11L, "Jenny", "BadBreath"));
		//this.countPopularFriends = 2;
		
		ResultSet rst = null; 
		PreparedStatement getPopularFriendsStmt = null;
		
		try {
			String getPopularFriendsSql = "SELECT user_id, first_name, last_name "
					+ "FROM " + userTableName + " WHERE user_id IN "
					+ "(SELECT user1_id FROM ( "
						+ "SELECT user1_id, user2_id from " + friendsTableName
						+ " UNION "
						+ "SELECT user2_id, user1_id from " + friendsTableName + ") "
						+ "GROUP BY user1_id HAVING COUNT(*)>80)";
			getPopularFriendsStmt = oracleConnection.prepareStatement(getPopularFriendsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			// getMonthCountSql is the query that will run
			// For each month, find the number of friends born that month
			// Sort them in descending order of count
			// executeQuery will run the query and generate the result set
			rst = getPopularFriendsStmt.executeQuery();
			
			this.countPopularFriends = 0;
			while(rst.next()) {
				this.popularFriends.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
				this.countPopularFriends++;
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getPopularFriendsStmt != null)
				getPopularFriendsStmt.close();
		}
	}


	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
		/* Catherine did this query */
		ResultSet rst = null;
		PreparedStatement getLiveAtHomeStmt = null;
		
		try {
			String getLiveAtHomeSql = "select U.USER_ID, FIRST_NAME, LAST_NAME from " + userTableName + 
					" U, " + currentCityTableName + " C, " + hometownCityTableName + " H" +
					" where CURRENT_CITY_ID = HOMETOWN_CITY_ID and U.USER_ID = C.USER_ID and U.USER_ID = H.USER_ID";
			getLiveAtHomeStmt = oracleConnection.prepareStatement(getLiveAtHomeSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = getLiveAtHomeStmt.executeQuery();
			
			int count = 0;
			while (rst.next()) {
				count++;
				UserInfo u = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
				this.liveAtHome.add(u);
			}
			this.countLiveAtHome = count;
			
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getLiveAtHomeStmt != null)
				getLiveAtHomeStmt.close();
		}
	}



	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
		String photoId = "1234567";
		String albumId = "123456789";
		String albumName = "album1";
		String photoCaption = "caption1";
		String photoLink = "http://google.com";
		PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
		TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
		tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
		tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
		this.photosWithMostTags.add(tp);
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
		Long girlUserId = 123L;
		String girlFirstName = "girlFirstName";
		String girlLastName = "girlLastName";
		int girlYear = 1988;
		Long boyUserId = 456L;
		String boyFirstName = "boyFirstName";
		String boyLastName = "boyLastName";
		int boyYear = 1986;
		MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		String sharedPhotoId = "12345678";
		String sharedPhotoAlbumId = "123456789";
		String sharedPhotoAlbumName = "albumName";
		String sharedPhotoCaption = "caption";
		String sharedPhotoLink = "link";
		mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
		this.bestMatches.add(mp);
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
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

		p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
		p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
		p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
		this.suggestedFriendsPairs.add(p);
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
		this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
		this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
		this.eventCount = 12;
		this.popularCityNames.add("Ann Arbor");
		this.popularCityNames.add("Ypsilanti");
	}
	
	
	
	@Override
//	 ***** Query 9 *****
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
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
		this.siblings.add(s);
	}
	
}
