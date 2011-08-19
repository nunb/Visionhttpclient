import java.net.UnknownHostException;

import com.mongodb.*;

public class mongotrial
{
	public static void main(String[] args) throws UnknownHostException, MongoException
	{

		Mongo m = new Mongo( "localhost" , 27017 );

		DB db = m.getDB( "crazydb" );
		//boolean auth = db.authenticate("admin", "admin");
		DBCollection coll = db.getCollection("testCollection");
        /*BasicDBObject doc = new BasicDBObject();

        doc.put("name", "MongoDB");
        doc.put("type", "database");
        doc.put("count", 1);

        BasicDBObject info = new BasicDBObject();

        info.put("x", 203);
        info.put("y", 102);

        doc.put("info", info);

        coll.insert(doc);*/
		
		DBObject myDoc = coll.findOne();
		System.out.println(myDoc);
		System.out.println("printing the number " +coll.getCount());
	}
}