package com.easylearnjava.dao;

import com.easylearnjava.exception.DaoException;
import com.easylearnjava.util.Constants;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class LoginDao {

	/**
	 * Method creates the database and collections if not exist and inserts 2 records in to the DB. 
	 * Also fetches password for the user name from the data base
	 * 
	 * @param userName
	 * @return
	 */
	public String getUserPassword(String userName) {
		
		MongoClient mongoClient;
		DBCollection collection;
		String passwordfromDB = null;
		try {
			// To connect to mongodb local server
			mongoClient = new MongoClient("localhost", 27017);

			// Now connect to your database -- if not exist, it will create athena db
			DB db = mongoClient.getDB("athena");
			
			//if user table(called as collection in mongodb) exists then get the connection
			//if user table does not exist, then create table and insert records
			if (db.collectionExists("user")) {
				collection = db.getCollection("user");
			} else {
				// creates a db with 2mb size
				DBObject options = BasicDBObjectBuilder.start()
						.add("capped", true).add("size", 2000000).get();
				collection = db.createCollection("user", options);

				//insert data into the collection
				BasicDBObject firstRecord = new BasicDBObject("_id", 1).append(
						"user_name", "raghu").append("user_password", "secret");
				collection.insert(firstRecord);

				BasicDBObject secondRecord = new BasicDBObject("_id", 2)
						.append("user_name", "naveen").append("user_password",
								"topsecret");
				collection.insert(secondRecord);
			}
			
			//adding a query with username restriction
			BasicDBObject query = new BasicDBObject("user_name", userName);
			
			//retrieves the record(called as document in mongodb) with the above mentioned condition in the query object
			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				DBObject doc = cursor.next();
				passwordfromDB = doc.get("user_password").toString();
				break;
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new DaoException(Constants.GLOBAL_EXCEPTION_MESSAGE);
		}

		return passwordfromDB;
	}

}
