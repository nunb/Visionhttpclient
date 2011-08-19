import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class RulesRequests
{
	
	public static String cookie;
	
	private static String server = "http://localhost:7070";
	
	private static String newAssetId;
	
	//private static String sensorAssetId = "4e0846a81776272b088428d4";
	
	private static String tagId = "80396453422";

	public static void main(String[] args)
	{
		//String server = "http://10.0.4.60:8585";
		String server = "http://localhost:7070";
		login(server+"/login","C:\\Users\\pranav.modi\\Desktop\\http\\post.txt");
		//System.outprintln("the cookie value is :"+cookie);
		//createRule(server+"/eventRules","C:\\Users\\pranav.modi\\Desktop\\http\\newdwellabsence.txt");4e4ce7c217763988d2c3dd03
		//createAssetType(server+"/assetTypes", "C:\\Users\\pranav.modi\\Desktop\\http\\assettype.txt");
		readRules(server+"/eventRules");
		//postAssets(server+"/assets","C:\\Users\\pranav.modi\\Desktop\\http\\assets7070.txt", "kasset");
		//sendMessage("http://localhost:7070/assets", "C:\\Users\\pranav.modi\\Desktop\\http\\sensorasset.txt");
		//sendMessage(server+"/eventSearch?skip=0&limit=30&sortfields=timestamp&order=desc", "C:\\Users\\pranav.modi\\Desktop\\http\\eventpost.txt");
		//sendMessage("http://localhost:7070/eventRules/sendTestEmail", "C:\\Users\\pranav.modi\\Desktop\\http\\emailtest.txt");
		//sendMessage(server+"/assets/tagMessages", "C:\\Users\\pranav.modi\\Desktop\\http\\messageformat.txt");
		//bindSensor(server+"/assets/4e3b90b9a4ace05218ed730a/tag",tagId);
	}
	
	public static void bindSensor(String target, String tagId)
	{
	   	try
    	{
	   		System.out.println(" The target is " +target);
            BufferedReader in = new BufferedReader(new FileReader("C:\\Users\\pranav.modi\\Desktop\\http\\bindsensor.txt"));
            StringBuffer strbuffer = new StringBuffer();
            String str;
            while ((str = in.readLine()) != null) 
            {
                strbuffer.append(str);
            }
            System.out.println("the data from file is : "+strbuffer);
            in.close();
            String content = strbuffer.toString();


        	System.out.println("About to post\nURL: "+target+ "\ncontent: " + content);
        	  StringBuffer response = new StringBuffer();
        	  URL url = new URL(target);
        	  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        	  
        	  conn.setRequestMethod("POST");
        	  //conn.setRequestProperty("X-Vision-REST-Method", "PUT");
        	  conn.setRequestProperty("Referer",server+"/Vision.swf/[[DYNAMIC]]/6");
        	  
        	  conn.setRequestProperty("Content-Type", "application/xml");
        	  conn.setRequestProperty("Cookie", cookie);
        	  //conn.setRequestProperty( "User-Agent", "Mozilla/4.0" );
        	  conn.setRequestProperty("Content-Length", content.length()+"");
        	  conn.setDoInput (true);
        	  conn.setDoOutput (true);
        	  conn.connect();

        	  DataOutputStream out = new DataOutputStream (conn.getOutputStream ());
        	  // Write out the bytes of the content string to the stream.
        	  out.writeBytes(content);
        	  out.flush();
        	  out.close();
        	  
        	  BufferedReader in1 = new BufferedReader (new InputStreamReader(conn.getInputStream ()));
        	  String temp;
        	  while ((temp = in1.readLine()) != null)
        	  {
        	    response.append(temp);
        	  }
        	  temp = null;
        	  in1.close ();
        	  System.out.println("Server response:\n'" + response.toString() + "'");
        	  extractAssetId(response.toString());
        	 
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
	}
	
	  public static void postAssets(String target, String filepath, String assetName)
	  {
		   	try
	    	{
	            BufferedReader in = new BufferedReader(new FileReader(filepath));
	            StringBuffer strbuffer = new StringBuffer();
	            String str;
	            while ((str = in.readLine()) != null) 
	            {
	                strbuffer.append(str);
	            }
	            //System.out.println("the data from file is : "+strbuffer);
	            in.close();
	            String content = strbuffer.toString();
	            
	  		  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			  DocumentBuilder db = dbf.newDocumentBuilder();
			  InputSource inStream = new org.xml.sax.InputSource();
			    
			  inStream.setCharacterStream(new java.io.StringReader(content));
			  Document doc = db.parse(inStream);
			  Element root = doc.getDocumentElement();
			  System.out.println("the root elements is : "+root);
			  NodeList propertyList = root.getElementsByTagName("property");
			  Element property = (Element)propertyList.item(0);
			  
			  property.setAttribute("value", assetName);
			 
	          TransformerFactory transfac = TransformerFactory.newInstance();
	          Transformer trans = transfac.newTransformer();
	          trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	          trans.setOutputProperty(OutputKeys.INDENT, "yes");

	          //create string from xml tree
	          StringWriter sw = new StringWriter();
	          StreamResult result = new StreamResult(sw);
	          DOMSource source = new DOMSource(doc);
	          trans.transform(source, result);
	          content = sw.toString();
			 
			  
			  
	        	System.out.println("About to post\nURL: "+target+ "\ncontent: " + content);
	        	  StringBuffer response = new StringBuffer();
	        	  URL url = new URL(target);
	        	  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	        	  
	        	  conn.setRequestMethod("POST");
	        	  //conn.setRequestProperty("X-Vision-REST-Method", "PUT");
	        	  conn.setRequestProperty("Referer",server+"/Vision.swf/[[DYNAMIC]]/6");
	        	  
	        	  conn.setRequestProperty("Content-Type", "application/xml");
	        	  conn.setRequestProperty("Cookie", cookie);
	        	  //conn.setRequestProperty( "User-Agent", "Mozilla/4.0" );
	        	  conn.setRequestProperty("Content-Length", content.length()+"");
	        	  conn.setDoInput (true);
	        	  conn.setDoOutput (true);
	        	  conn.connect();

	        	  DataOutputStream out = new DataOutputStream (conn.getOutputStream ());
	        	  // Write out the bytes of the content string to the stream.
	        	  out.writeBytes(content);
	        	  out.flush();
	        	  out.close();
	        	  
	        	  BufferedReader in1 = new BufferedReader (new InputStreamReader(conn.getInputStream ()));
	        	  String temp;
	        	  while ((temp = in1.readLine()) != null)
	        	  {
	        	    response.append(temp);
	        	  }
	        	  temp = null;
	        	  in1.close ();
	        	  System.out.println("Server response:\n'" + response.toString() + "'");
	        	  extractAssetId(response.toString());
	        	 
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}

	  }
	  
	  public static void extractAssetId(String response)
	  {
		  try
		  {
			  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			  DocumentBuilder db = dbf.newDocumentBuilder();
			  InputSource inStream = new org.xml.sax.InputSource();
			    
			  inStream.setCharacterStream(new java.io.StringReader(response));
			  Document doc = db.parse(inStream);
			  Element root = doc.getDocumentElement();

			  newAssetId = root.getAttribute("id");
		  }
		  catch(Exception e)
		  {
			  e.printStackTrace();
		  }

	  }
	
	
	 public static void login(String target, String filepath)
	  {
	    	try
	    	{
	            BufferedReader in = new BufferedReader(new FileReader(filepath));
	            StringBuffer strbuffer = new StringBuffer();
	            String str;
	            while ((str = in.readLine()) != null) 
	            {
	                strbuffer.append(str);
	            }
	            //System.out.println("the data from file is : "+strbuffer);
	            in.close();
	            String content = strbuffer.toString();
	        	System.out.println("About to post\nURL: "+target+ "\ncontent: " + content);
	        	  StringBuffer response = new StringBuffer();
	        	  URL url = new URL(target);
	        	  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	        	  
	        	  conn.setRequestMethod("POST");
	        	  conn.setRequestProperty("X-Vision-REST-Method", "PUT");
	        	  
	        	  conn.setRequestProperty("Content-Type", "application/xml");
	        	  conn.setRequestProperty( "User-Agent", "Mozilla/4.0" );
	        	  conn.setRequestProperty("Content-Length", content.length()+"");
	
	        	  
	        	  conn.setDoInput (true);
	        	  conn.setDoOutput (true);
	
	        	  conn.connect();
	
	        	  DataOutputStream out = new DataOutputStream (conn.getOutputStream ());
	        	  // Write out the bytes of the content string to the stream.
	        	  out.writeBytes(content);
	        	  out.flush ();
	        	  out.close ();
	        	  
	        	  
	        	  // Read response from the input stream.
	        	  Map<String, List<String>> headers = conn.getHeaderFields(); 
	        	  
	        	  
	        	  List<String> values = headers.get("Set-Cookie"); 
	        	  
	        	  String cookieVal = values.get(0);
	        	 //System.out.println("cookieval : "+cookieVal);
	        	  
	        	  cookie = cookieVal.split(";")[0];
	        	  
	        	  //System.out.println("cookie is : "+cookie);
	        	  
	        	  BufferedReader in1 = new BufferedReader (new InputStreamReader(conn.getInputStream ()));
	        	  String temp;
	        	  while ((temp = in1.readLine()) != null){
	        	    response.append(temp);
	        	   }
	        	  temp = null;
	        	  in1.close ();
	        	  System.out.println("Server response:\n'" + response.toString() + "'");	        	 
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	   }
	 
	  public static void readRules(String url)
	    {
	    	try
	    	{
	            URL vision = new URL(url);
	            URLConnection yc = vision.openConnection();
	            yc.setRequestProperty("X-Vision-REST-Method", "PUT");
	            yc.setRequestProperty("Cookie", cookie);
	            BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
	            StringBuffer data = new StringBuffer();
	            String inputLine;
	            while ((inputLine = in.readLine()) != null) 
	                data.append(inputLine);
	            in.close();
	            System.out.println("Data : "+data);
	    	}
	    	catch (Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    }
	
	  public static void sendMessage(String target, String filepath)
	  {
	    	try
	    	{
	            BufferedReader in = new BufferedReader(new FileReader(filepath));
	            StringBuffer strbuffer = new StringBuffer();
	            String str;
	            while ((str = in.readLine()) != null) 
	            {
	                strbuffer.append(str);
	            }
	            //System.out.println("the data from file is : "+strbuffer);
	            in.close();
	            String content = strbuffer.toString();
	            //content="";
	        	System.out.println("About to post\nURL: "+target+ "\ncontent: " + content);
	        	  StringBuffer response = new StringBuffer();
	        	  URL url = new URL(target);
	        	  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	        	  
	        	  conn.setRequestMethod("POST");
	        	  //conn.setRequestProperty("X-Vision-REST-Method", "PUT");
	        	  conn.setRequestProperty("Cookie", cookie);
	        	  conn.setRequestProperty("Content-Type", "application/xml");
	        	  conn.setRequestProperty( "User-Agent", "Mozilla/4.0" );
	        	  conn.setRequestProperty("Content-Length", content.length()+"");

	        	  conn.setDoInput (true);
	        	  conn.setDoOutput (true);

	        	  conn.connect();

	        	  DataOutputStream out = new DataOutputStream (conn.getOutputStream ());
	        	  // Write out the bytes of the content string to the stream.
	        	  out.writeBytes(content);
	        	  out.flush ();
	        	  out.close ();

	        	  BufferedReader in1 = new BufferedReader (new InputStreamReader(conn.getInputStream()));
	        	  String temp;
	        	  while ((temp = in1.readLine()) != null){
	        	    response.append(temp);
	        	   }
	        	  temp = null;
	        	  in1.close ();
	        	  System.out.println("Server response:\n'" + response.toString() + "'");
	        	 
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	  }
	  
	  public static void createRule(String target, String filepath)
	  {
	    	try
	    	{
	            BufferedReader in = new BufferedReader(new FileReader(filepath));
	            StringBuffer strbuffer = new StringBuffer();
	            String str;
	            while ((str = in.readLine()) != null) 
	            {
	                strbuffer.append(str);
	            }
	            //System.out.println("the data from file is : "+strbuffer);
	            in.close();
	            String content = strbuffer.toString();
	            //content="";
	        	System.out.println("About to post\nURL: "+target+ "\ncontent: " + content);
	        	  StringBuffer response = new StringBuffer();
	        	  URL url = new URL(target);
	        	  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	        	  
	        	  conn.setRequestMethod("POST");
	        	  //conn.setRequestProperty("X-Vision-REST-Method", "PUT");
	        	  conn.setRequestProperty("Cookie", cookie);
	        	  conn.setRequestProperty("Content-Type", "application/xml");
	        	  conn.setRequestProperty( "User-Agent", "Mozilla/4.0" );
	        	  conn.setRequestProperty("Content-Length", content.length()+"");

	        	  conn.setDoInput (true);
	        	  conn.setDoOutput (true);

	        	  conn.connect();

	        	  DataOutputStream out = new DataOutputStream (conn.getOutputStream ());
	        	  // Write out the bytes of the content string to the stream.
	        	  out.writeBytes(content);
	        	  out.flush ();
	        	  out.close ();

	        	  BufferedReader in1 = new BufferedReader (new InputStreamReader(conn.getInputStream()));
	        	  String temp;
	        	  while ((temp = in1.readLine()) != null){
	        	    response.append(temp);
	        	   }
	        	  temp = null;
	        	  in1.close ();
	        	  System.out.println("Server response:\n'" + response.toString() + "'");
	        	 
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	  }
	  
	  public static void createAssetType(String target, String filepath)
	  {
	    	try
	    	{
	            BufferedReader in = new BufferedReader(new FileReader(filepath));
	            StringBuffer strbuffer = new StringBuffer();
	            String str;
	            while ((str = in.readLine()) != null) 
	            {
	                strbuffer.append(str);
	            }
	            //System.out.println("the data from file is : "+strbuffer);
	            in.close();
	            String content = strbuffer.toString();
	            //content="";
	        	System.out.println("About to post\nURL: "+target+ "\ncontent: " + content);
	        	  StringBuffer response = new StringBuffer();
	        	  URL url = new URL(target);
	        	  HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	        	  
	        	  conn.setRequestMethod("POST");
	        	  //conn.setRequestProperty("X-Vision-REST-Method", "PUT");
	        	  conn.setRequestProperty("Cookie", cookie);
	        	  conn.setRequestProperty("Content-Type", "application/xml");
	        	  conn.setRequestProperty( "User-Agent", "Mozilla/4.0" );
	        	  conn.setRequestProperty("Content-Length", content.length()+"");

	        	  conn.setDoInput (true);
	        	  conn.setDoOutput (true);

	        	  conn.connect();

	        	  DataOutputStream out = new DataOutputStream (conn.getOutputStream ());
	        	  // Write out the bytes of the content string to the stream.
	        	  out.writeBytes(content);
	        	  out.flush ();
	        	  out.close ();

	        	  BufferedReader in1 = new BufferedReader (new InputStreamReader(conn.getInputStream()));
	        	  String temp;
	        	  while ((temp = in1.readLine()) != null){
	        	    response.append(temp);
	        	   }
	        	  temp = null;
	        	  in1.close ();
	        	  System.out.println("Server response:\n'" + response.toString() + "'");
	        	 
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	  }
}