package pdpthree.mr3;

import java.io.StringReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Parser {
	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	public static  Node PreProcess(String line, String pageName) {
		try {
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names.
			List<String> linkPageNames = new LinkedList<>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

			// Each line formatted as (Wiki-page-name:Wiki-page-html).
			line = line.replaceAll("&(?!amp;)", "&amp;");

			int delimLoc = line.indexOf(':');
			
			String html = line.substring(delimLoc + 1);

			Matcher matcher = namePattern.matcher(pageName);
			if (!matcher.find()) {
				// Skip this html file, name contains (~).
				return null;
			}

			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
				return null;
			}
			
			// set to remove redundant outlinks
			Set<String> uniqueLinks = new HashSet<String>(linkPageNames);
		
			StringBuilder listOfNode = new StringBuilder("");
			String lon=null;
			if(!uniqueLinks.isEmpty()) {
				for(String s:uniqueLinks) {
					// remove self links
					if(!s.equals(pageName)) {
						listOfNode.append(s);
						listOfNode.append(",");
					}
				}
				if(!listOfNode.toString().equals("")) {
					lon = listOfNode.toString().substring(0, listOfNode.length()-1);
				}
				else {
					// handle nodes with empty adjlist
					lon=" ";
				}
			}
			else {
				// handle nodes with empty adjlist
				lon=" ";
			}
			
			// create new node
			Node n = new Node(new Text(pageName),new Text(lon));
			return n;
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {

		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}

				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) { 
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
}
