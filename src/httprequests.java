import java.net.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class httprequests 
{
	private static String cookie;
	
	private static String Vijay2 = "301B-1038-33672";
	private static String Vijay2Tag = "105463710599";
	

	private static String Vijay3 = "301B-1038-33673";
	private static String Vijay3Tag = "105463710600";
	
	private static String Vijay1 = "301B-1038-33668";
	private static String Vijay1Tag = "105463710595";
	
	private static String Ekahau9f = "301A-1004-130345";
	private static String Ekahau9fTag = "105462134431";
	
	private static String ktemp = "A09470F000000000";
	private static String ktempTag = "80396464283";
	
	private static String krishna1 = "301B-1038-33651";
	private static String krishna1Tag = "105463710578";
	
	private static String krishna2 = "301B-1038-33687";
	private static String krishna2Tag = "105463710614";
	
	private static String k6 = "301B-1038-33668";
	private static String k6Tag = "105463710595";
	
	private static String hessu = "hessu";
	private static String hessuTag = "1";
	
	private static String tag708e = "301B-1021-28815";
	private static String tag708etag = "105463705742";
	
	private static String ats1="ATS-2";
	private static String ats1Tag = "2";
	
	private static String a3 = "301B-1021-28836";
	private static String a3Tag = "105463705763";
	
	private static String simtag = tag708e;
	private static String simtagid = tag708etag;
	
	private static String newAssetId = "4e451c0638fa582c9c6654cf";
	
	private static String tagId=simtagid;
	//private static String tagId;
	
	private static List<String> freeTags = new LinkedList<String>();
	
	private static String base = "morebadass";
	
	private static String server = "http://localhost:7070";
	
	//private static List<String> tagIds = new LinkedList<String>();

	public static void main(String[] args)
    {
		 String filepath = "C:\\Users\\pranav.modi\\Desktop\\http\\post.txt";
		 String loginurl = server+"/login";
		 login(loginurl, filepath);
		 getTags(server+"/tags");
		 
		 for (int i=0;i<freeTags.size();i++)
		 {
			//System.out.println(freeTags.get(i));
			if (freeTags.get(i).equals(simtag))
			{
				searchTag(server+"/tags/search",freeTags.get(i)); 
				String bindUrl = server+"/assets/" + newAssetId+ "/tag";
				String bindPost = "<tag _method=\"PUT\" id=\""+tagId+"\"/>";
				bindTags(bindUrl,bindPost); 			
			}
			//searchTag(server+"/tags/search",freeTags.get(i)); 
			//postAssets(server+"/assets","C:\\Users\\pranav.modi\\Desktop\\http\\assets7070.txt", base+i);
			//String bindUrl = server+"/assets/" + newAssetId+ "/tag";
			//String bindPost = "<tag _method=\"PUT\" id=\""+tagId+"\"/>";
			//bindTags(bindUrl,bindPost); 			
		 }
		 //readAssets("http://localhost:7070/assets");

    }
    
  public static void readAssets(String url)
    {
    	try
    	{
            URL vision = new URL(url);
            URLConnection yc = vision.openConnection();
            yc.setRequestProperty("X-Vision-REST-Method", "PUT");
            yc.setRequestProperty("Cookie", cookie);
            BufferedReader in = new BufferedReader(
                                    new InputStreamReader(
                                    yc.getInputStream()));
            String inputLine;

            while ((inputLine = in.readLine()) != null) 
                System.out.println(inputLine);
            in.close();
    	}
    	catch (Exception e)
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
        	  out.flush();
        	  out.close();
        	  
        	  
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
    
  public static void searchTag(String target, String tagSN)
  {
	   	try
    	{

            String content = "<search text=\""+tagSN+"\"/>";
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
        	  System.out.println("Server response for tags:\n'" + response.toString() + "'");
    		  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    		  DocumentBuilder db = dbf.newDocumentBuilder();
    		  InputSource inStream = new org.xml.sax.InputSource();
    		    
    		  inStream.setCharacterStream(new java.io.StringReader(response.toString()));
    		  Document doc = db.parse(inStream);
    		  Element root = doc.getDocumentElement();
    		  System.out.println("the root elements is : "+root);
    		  NodeList tagElement = root.getElementsByTagName("tag");
    		  tagId = ((Element)tagElement.item(0)).getAttribute("tagid");
    		  System.out.println("the tagid is "+tagId);
    		  System.out.println("the tagid is : "+tagId);
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
  
  public static void getTags(String url)
  {
  	try
	{
        URL vision = new URL(url);
        URLConnection yc = vision.openConnection();
        yc.setRequestProperty("X-Vision-REST-Method", "PUT");
        yc.setRequestProperty("Cookie", cookie);
        BufferedReader in = new BufferedReader( new InputStreamReader(yc.getInputStream()));
        
        StringBuffer tagsXml = new StringBuffer();
        String inputLine;

        while ((inputLine = in.readLine()) != null) 
            tagsXml.append(inputLine);
        in.close();
        System.out.println("the tagsxml is : "+tagsXml.toString());
		  DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		  DocumentBuilder db = dbf.newDocumentBuilder();
		  InputSource inStream = new org.xml.sax.InputSource();
		    
		  inStream.setCharacterStream(new java.io.StringReader(tagsXml.toString()));
		  Document doc = db.parse(inStream);
		  Element root = doc.getDocumentElement();
		  System.out.println("the root elements is : "+root);
		  NodeList tagElements = root.getElementsByTagName("tag");
		  for (int i=0;i< tagElements.getLength();i++)
		  {
			  Element tag = (Element) tagElements.item(i);
			  if (!tag.hasAttribute("assetId"))
			  {
				  String tagName = tag.getAttribute("serialnumber");
				  freeTags.add(tagName);
			  }
			  
		  }
	}
	catch (Exception e)
	{
		e.printStackTrace();
	}
  }
  
  public static void bindTags(String target, String content)
  {
	   	try
    	{

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

    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
  }
}