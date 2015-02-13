package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
       /* Catherine did this query */
		ResultSet namesResult = null;
		PreparedStatement getNamesStmt = null;
		
		try {
			String getNamesSql = "select LAST_NAME, COUNT(*) from " + userTableName +
				" group by LAST_NAME order by length(LAST_NAME) desc, COUNT(*) desc" ;
			getNamesStmt = oracleConnection.prepareStatement(getNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			namesResult = getNamesStmt.executeQuery();
			
			int longest = 0;
			int common = 0;
			while(namesResult.next()) {
				if (namesResult.getInt(2) > common)
					common = namesResult.getInt(2);
				int length = namesResult.getString(1).length();
				if (namesResult.isFirst()) {
					longest = length;
					this.longestLastNames.add(namesResult.getString(1));
				}	
				else if (length == longest)
					this.longestLastNames.add(namesResult.getString(1));
			}
			
			int shortest = 0;
			namesResult.setFetchDirection(ResultSet.FETCH_REVERSE);
			while(namesResult.previous()) {
				int length = namesResult.getString(1).length();
				if (namesResult.isLast()) {
					shortest = length;
					this.shortestLastNames.add(namesResult.getString(1));
				}
				else if (length == shortest)
					this.shortestLastNames.add(namesResult.getString(1));
			}
			
			while (namesResult.next()) {
				if (namesResult.getInt(2) == common)
					this.mostCommonLastNames.add(namesResult.getString(1));
			}
			this.mostCommonLastNamesCount = common;
			
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(namesResult != null) 
				namesResult.close();
			
			if(getNamesStmt != null)
				getNamesStmt.close();
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
	public void popularFriends() throws SQLException {
		//Rachael did this query
		ResultSet popularResult = null; 
		PreparedStatement getPopularFriendsStmt = null;
		
		try {
			String getPopularFriendsSql = "SELECT user_id, first_name, last_name "
					+ "FROM " + userTableName + " WHERE user_id IN "
					+ "(SELECT user1_id FROM ( "
						+ "SELECT user1_id from " + friendsTableName
						+ " UNION ALL "
						+ "SELECT user2_id from " + friendsTableName + ") "
						+ "GROUP BY user1_id HAVING COUNT(*)>80)";
			getPopularFriendsStmt = oracleConnection.prepareStatement(getPopularFriendsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			popularResult = getPopularFriendsStmt.executeQuery();
			
			this.countPopularFriends = 0;
			while(popularResult.next()) {
				this.popularFriends.add(new UserInfo(popularResult.getLong(1), popularResult.getString(2), popularResult.getString(3)));
				this.countPopularFriends++;
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(popularResult != null) 
				popularResult.close();
			
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
		ResultSet homeResult = null;
		PreparedStatement getLiveAtHomeStmt = null;
		
		try {
			String getLiveAtHomeSql = "select U.USER_ID, FIRST_NAME, LAST_NAME from " + userTableName + 
					" U, " + currentCityTableName + " C, " + hometownCityTableName + " H" +
					" where CURRENT_CITY_ID = HOMETOWN_CITY_ID and U.USER_ID = C.USER_ID and U.USER_ID = H.USER_ID";
			getLiveAtHomeStmt = oracleConnection.prepareStatement(getLiveAtHomeSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			homeResult = getLiveAtHomeStmt.executeQuery();
			
			int count = 0;
			while (homeResult.next()) {
				count++;
				UserInfo u = new UserInfo(homeResult.getLong(1), homeResult.getString(2), homeResult.getString(3));
				this.liveAtHome.add(u);
			}
			this.countLiveAtHome = count;
			
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(homeResult != null) 
				homeResult.close();
			
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
		// Rachael did this query
		ResultSet tagsResult = null; 
		ResultSet photosResult = null;
		ResultSet usersResult = null;
		PreparedStatement getTaggedPhotoIDsStmt = null;
		PreparedStatement getPhotoInfoStmt = null;
		PreparedStatement getTaggedUsersStmt = null;
		
		try {
			String getTaggedPhotoIDsSql = "SELECT tag_photo_id "
					+ "FROM " + tagTableName + " GROUP BY tag_photo_id "
					+ "ORDER BY COUNT(tag_photo_id) DESC, tag_photo_id ASC";

			getTaggedPhotoIDsStmt = oracleConnection.prepareStatement(getTaggedPhotoIDsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			tagsResult = getTaggedPhotoIDsStmt.executeQuery();
			
			
			while(tagsResult.next()  && n > 0) {
				
				String getPhotoInfoSql = "SELECT P.photo_id, A.album_id, A.album_name, P.photo_caption, P.photo_link"
						+ " FROM " + photoTableName + " P, " + albumTableName + " A "
						+ " WHERE P.photo_id = ? AND P.album_id=A.album_id";
				getPhotoInfoStmt = oracleConnection.prepareStatement(getPhotoInfoSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				getPhotoInfoStmt.setString(1, tagsResult.getString(1));
				photosResult = getPhotoInfoStmt.executeQuery();
				if (photosResult.next())
				{
					PhotoInfo p = new PhotoInfo(photosResult.getString(1), photosResult.getString(2), photosResult.getString(3), photosResult.getString(4), photosResult.getString(5));
					TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
					
					String getTaggedUsersSql = "SELECT user_id, first_name, last_name FROM " + userTableName + 
							" WHERE user_id IN (SELECT tag_subject_id FROM " + tagTableName + " WHERE tag_photo_id = ?)";
					getTaggedUsersStmt = oracleConnection.prepareStatement(getTaggedUsersSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					getTaggedUsersStmt.setString(1, tagsResult.getString(1));
					
					usersResult = getTaggedUsersStmt.executeQuery();
					
					while (usersResult.next())
					{
						tp.addTaggedUser(new UserInfo(usersResult.getLong(1), usersResult.getString(2), usersResult.getString(3)));
					}
					
					this.photosWithMostTags.add(tp);
				}
				n--;
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(tagsResult != null) 
				tagsResult.close();
			if(photosResult != null) 
				photosResult.close();
			if(usersResult != null) 
				usersResult.close();
			
			if(getTaggedPhotoIDsStmt != null)
				getTaggedPhotoIDsStmt.close();
			if(getPhotoInfoStmt != null)
				getPhotoInfoStmt.close();
			if(getTaggedUsersStmt != null)
				getTaggedUsersStmt.close();
		}
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
		/* Catherine did this query */
		ResultSet matchesResult = null;
		ResultSet tagResult = null;
		PreparedStatement getMatchesStmt = null;
		PreparedStatement getTaggedMatchesStmt = null;
		
		try {
			String getMatchesSql = "select A.USER_ID, A.FIRST_NAME, A.LAST_NAME, A.YEAR_OF_BIRTH, B.USER_ID, B.FIRST_NAME, B.LAST_NAME, B.YEAR_OF_BIRTH from " 
					+ userTableName + " A, " + userTableName + " B, " + tagTableName + " S, " + tagTableName + " T"
					+ " where not exists (select USER1_ID, USER2_ID from " + friendsTableName + " where (A.USER_ID = USER1_ID and B.USER_ID = USER2_ID))"
					+ " and not exists (select USER1_ID, USER2_ID from " + friendsTableName + " where (A.USER_ID = USER2_ID and B.USER_ID = USER1_ID))"
					+ " and (A.GENDER = 'female' and B.GENDER = 'male') and (ABS(A.YEAR_OF_BIRTH - B.YEAR_OF_BIRTH) <= ?"
					+ ") and (A.USER_ID = S.TAG_SUBJECT_ID and B.USER_ID = T.TAG_SUBJECT_ID and S.TAG_PHOTO_ID = T.TAG_PHOTO_ID)"
					+ " order by A.USER_ID asc, B.USER_ID desc";
			getMatchesStmt = oracleConnection.prepareStatement(getMatchesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			getMatchesStmt.setInt(1, yearDiff);
			matchesResult = getMatchesStmt.executeQuery();
			
			while (matchesResult.next()) {
				long userA = matchesResult.getLong(1);
				long userB = matchesResult.getLong(5);
				MatchPair mp = new MatchPair(userA, matchesResult.getString(2), matchesResult.getString(3), 
						matchesResult.getInt(4), userB, matchesResult.getString(6), matchesResult.getString(7), matchesResult.getInt(8));
				
				String getTaggedMatchesSql = "select PHOTO_ID, P.ALBUM_ID, ALBUM_NAME, PHOTO_CAPTION, PHOTO_LINK from " + photoTableName
						+ " P, " + albumTableName + " A, " + tagTableName + " S, " + tagTableName + " T" 
						+ " where (P.ALBUM_ID = A.ALBUM_ID) and (S.TAG_PHOTO_ID = T.TAG_PHOTO_ID and S.TAG_PHOTO_ID = P.PHOTO_ID)"
						+ " and (S.TAG_SUBJECT_ID = ? and T.TAG_SUBJECT_ID = ?)";
				getTaggedMatchesStmt = oracleConnection.prepareStatement(getTaggedMatchesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				getTaggedMatchesStmt.setLong(1, userA);
				getTaggedMatchesStmt.setLong(2, userB);
				tagResult = getTaggedMatchesStmt.executeQuery();
				
				while (tagResult.next()) {
					mp.addSharedPhoto(new PhotoInfo(tagResult.getString(1), tagResult.getString(2), 
							tagResult.getString(3), tagResult.getString(4),tagResult.getString(5)));
				}
				this.bestMatches.add(mp);
			}
					
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(matchesResult != null) 
				matchesResult.close();
			
			if(getMatchesStmt != null)
				getMatchesStmt.close();
			
			if(getTaggedMatchesStmt != null)
				getTaggedMatchesStmt.close();
		}
	}

	
	@Override
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
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
		// Rachael did this query
		ResultSet pairsResult = null; 
		ResultSet mutualsResult = null;
		PreparedStatement makeViewStmt = null;
		PreparedStatement getPairsStmt = null;
		PreparedStatement getDeetsStmt = null;
		PreparedStatement dropViewStmt = null;
		
		try {
			//Find all users C who are friends with both A and B where A and B are not friends 
			String makeViewSql = "create or replace view potentialpairs as ("
					+ "select A.first_name as a_fn, A.last_name as a_ln, B.first_name as b_fn, B.last_name as b_ln, "
					+ "C.first_name as c_fn, C.last_name as c_ln, A.user_id as a_id, B.user_id as b_id, C.user_id as c_id "
					+ " from " + userTableName + " A, " + userTableName + " B, " + userTableName + " C, "
							+ friendsTableName + " f1, " + friendsTableName + " f2 "
					+ " where A.user_id < B.user_id AND "
					+ " not exists (select user1_id from " + friendsTableName + " fa "
							+ " where A.user_id = fa.user1_id and B.user_id = fa.user2_id) "
					// Handle cases of friendship (AC)(BC), (AC)(CB), (AB)(BC), (AB)(CB)
					+ " and ((A.user_id = f1.user1_id and f1.user2_id = C.user_id and C.user_id = f2.user1_id and f2.user2_id = B.user_id) or "
					+ " (A.user_id = f1.user1_id and f1.user2_id = C.user_id and C.user_id = f2.user2_id and f2.user1_id = B.user_id) or "
					+ " (A.user_id = f1.user2_id and f1.user1_id = C.user_id and C.user_id = f2.user1_id and f2.user2_id = B.user_id) or "
					+ " (A.user_id = f1.user2_id and f1.user1_id = C.user_id and C.user_id = f2.user2_id and f2.user1_id = B.user_id))) ";
			
			makeViewStmt = oracleConnection.prepareStatement(makeViewSql);
			
			makeViewStmt.execute();
			
			String getPairsSql = "select a_id, b_id "
					+ "from potentialpairs group by a_id, b_id order by count(*) desc, a_id, b_id";
			getPairsStmt = oracleConnection.prepareStatement(getPairsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			pairsResult = getPairsStmt.executeQuery();

			while(pairsResult.next() && n > 0) {
				Long user1_id = pairsResult.getLong(1);
				Long user2_id = pairsResult.getLong(2);
				
				String getDeetsSql = "select a_fn, a_ln, b_fn, b_ln, c_id, c_fn, c_ln from potentialpairs "
						+ " where a_id = ? and b_id = ?";
				getDeetsStmt = oracleConnection.prepareStatement(getDeetsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				getDeetsStmt.setLong(1, user1_id);
				getDeetsStmt.setLong(2, user2_id);
				mutualsResult = getDeetsStmt.executeQuery();
				if (mutualsResult.next())
				{
					String user1FirstName = mutualsResult.getString(1);
					String user1LastName = mutualsResult.getString(2);
					String user2FirstName = mutualsResult.getString(3);
					String user2LastName = mutualsResult.getString(4);
					FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
					
					do{
						p.addSharedFriend(mutualsResult.getLong(5), mutualsResult.getString(6), mutualsResult.getString(7));
					}while (mutualsResult.next());
					
					this.suggestedFriendsPairs.add(p);
				}
				n--;
			}
			String dropViewSql = "drop view potentialpairs";
			dropViewStmt = oracleConnection.prepareStatement(dropViewSql);
			dropViewStmt.execute();

		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(pairsResult != null) 
				pairsResult.close();
			if(mutualsResult != null) 
				mutualsResult.close();
			if(makeViewStmt != null)
				makeViewStmt.close();
			if(getPairsStmt != null)
				getPairsStmt.close();
			if(getDeetsStmt != null)
				getDeetsStmt.close();
			if(dropViewStmt != null)
				dropViewStmt.close();

		}
	}
	
	
	@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {
		/* Catherine did this query */
		ResultSet agesResult = null;
		PreparedStatement getFriendsStmt = null;
	
		try {
			String getFriendsSql = "select USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH from " + userTableName +
					" where USER_ID in (select USER2_ID from " + friendsTableName +
										" where USER1_ID = ? union select USER1_ID from " + friendsTableName + 
										" where USER2_ID = ?)" +
					" order by YEAR_OF_BIRTH asc, MONTH_OF_BIRTH asc, DAY_OF_BIRTH asc, USER_ID desc";
			getFriendsStmt = oracleConnection.prepareStatement(getFriendsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			getFriendsStmt.setLong(1, user_id);
			getFriendsStmt.setLong(2, user_id);
			agesResult = getFriendsStmt.executeQuery();
			
			while (agesResult.next()) {
				if (agesResult.isFirst())
					this.oldestFriend = new UserInfo(agesResult.getLong(1), agesResult.getString(2), agesResult.getString(3));
				if (agesResult.isLast())
					this.youngestFriend = new UserInfo(agesResult.getLong(1), agesResult.getString(2), agesResult.getString(3));
			}
			
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(agesResult != null) 
				agesResult.close();
			
			if(getFriendsStmt != null)
				getFriendsStmt.close();
		}
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	
	public void findEventCities() throws SQLException {
		// Rachael did this query
		ResultSet citiesResult = null; 
		ResultSet namesResult = null;
		PreparedStatement getTopCityIdsStmt = null;
		PreparedStatement getTopCityNameStmt = null;
		
		try {
			String getTopCityIdsSql = "SELECT event_city_id, COUNT(*) FROM " + eventTableName + 
			" GROUP BY event_city_id ORDER BY COUNT(*) DESC";

			getTopCityIdsStmt = oracleConnection.prepareStatement(getTopCityIdsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			citiesResult = getTopCityIdsStmt.executeQuery();
			
			
			if (!citiesResult.next())
				return;
			
			do {
				this.eventCount = citiesResult.getInt(2);
				String id = citiesResult.getString(1);
				String getTopCityNameSql = "SELECT city_name FROM " + cityTableName + " WHERE city_id = ?";
				getTopCityNameStmt = oracleConnection.prepareStatement(getTopCityNameSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				getTopCityNameStmt.setString(1,  citiesResult.getString(1));
				namesResult = getTopCityNameStmt.executeQuery();
				
				while (namesResult.next())
					this.popularCityNames.add(namesResult.getString(1));
				
			} while(citiesResult.next() && citiesResult.getInt(2) == this.eventCount);
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(citiesResult != null) 
				citiesResult.close();
			if(namesResult != null) 
				namesResult.close();
			if(getTopCityIdsStmt != null)
				getTopCityIdsStmt.close();
			if(getTopCityNameStmt != null)
				getTopCityNameStmt.close();
		}
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
		/* Catherine did this query */
		ResultSet siblingsResult = null;
		PreparedStatement getPotentialSibsStmt = null;
		
		try {
			String getPotentialSibsSql = "select A.USER_ID, A.FIRST_NAME, A.LAST_NAME, B.USER_ID, B.FIRST_NAME, B.LAST_NAME from "
					+ userTableName + " A, " + userTableName + " B, " + friendsTableName + ", " + hometownCityTableName + " C, " + hometownCityTableName + 
					" D where (A.USER_ID < B.USER_ID and A.USER_ID = USER1_ID and B.USER_ID = USER2_ID)"  +
					" and ABS(A.YEAR_OF_BIRTH - B.YEAR_OF_BIRTH) < 10 and (A.LAST_NAME = B.LAST_NAME) and (A.USER_ID = C.USER_ID and B.USER_ID = D.USER_ID and" +
					" C.HOMETOWN_CITY_ID = D.HOMETOWN_CITY_ID) order by A.USER_ID asc, B.USER_ID asc";
			getPotentialSibsStmt = oracleConnection.prepareStatement(getPotentialSibsSql,  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			siblingsResult = getPotentialSibsStmt.executeQuery();
			
			while (siblingsResult.next()) {
				SiblingInfo s = new SiblingInfo(siblingsResult.getLong(1), siblingsResult.getString(2), siblingsResult.getString(3), siblingsResult.getLong(4), siblingsResult.getString(5), siblingsResult.getString(6));
				this.siblings.add(s);
			}
			
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(siblingsResult != null) 
				siblingsResult.close();
			
			if(getPotentialSibsStmt != null)
				getPotentialSibsStmt.close();
		}
	}
	
}

