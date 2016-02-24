package es.udc.fic.ri.P1.ej2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrentIndexator {
	public static class CACMParser {

		/*
		 * CACM Parser by Jorge Nuñez
		 */

		public static List<List<String>> parseString(StringBuffer fileContent) {
			/* First the contents are converted to a string */
			String text = fileContent.toString();

			/*
			 * The method split of the String class splits the strings using the
			 * delimiter which was passed as argument Therefor lines is an array
			 * of strings, one string for each line
			 */
			String[] lines = text.split("\n");

			/*
			 * For each CACM article the parser returns a list of strings where
			 * each element of the list is a field ( docid (.I), title (.T),
			 * abstract (.W), date (.B), authors (.A), keywords (.K), entrydate
			 * (.N)). Each cacm-???.all file that is passed in fileContent can
			 * contain many CACM articles, so finally the parser returns a list
			 * of list of strings, i.e, a list of CACM articles, that is what
			 * the object documents contains
			 */

			List<List<String>> documents = new LinkedList<List<String>>();

			/* The tag .I identifies the beginning of each article */

			for (int i = 0; i < lines.length; ++i) {
				if (!lines[i].startsWith(".I"))
					continue;
				StringBuilder sb = new StringBuilder();
				sb.append(lines[i++]);
				sb.append("\n");
				while (i < lines.length && !lines[i].startsWith(".I")) {
					sb.append(lines[i++]);
					sb.append("\n");
				}
				i--;
				/*
				 * Here the sb object of the StringBuilder class contains the
				 * CACM article which is converted to text and passed to the
				 * handle document method that will return the document in the
				 * form of a list of fields
				 */
				documents.add(handleDocument(sb.toString()));
			}
			return documents;
		}

		public static List<String> handleDocument(String text) {

			/*
			 * This method returns the CACM article that is passed as text as a
			 * list of fields
			 */

			/*
			 * The fields docid (.I), title (.T), abstract (.W), date (.B),
			 * authors (.A), keywords
			 */
			/* .K), entrydate (.N). are extracted */

			String docid = extract("I", text, true);
			String title = extract("T\n", text, true);
			String sabstract = extract("W\n", text, true);
			String date = extract("B\n", text, true);
			String authors = extract("A\n", text, true);
			String keywords = extract("K\n", text, true);
			String entrydate = extract("N\n", text, true);

			List<String> document = new LinkedList<String>();
			document.add(docid);
			document.add(title);
			document.add(sabstract);
			document.add(date);
			document.add(authors);
			document.add(keywords);
			document.add(entrydate);
			return document;
		}

		private static String extract(String elt, String text,
				boolean allowEmpty) {

			/*
			 * This method find the tags for the field elt in the String text
			 * and extracts and returns the content
			 */
			/*
			 * If the tag does not exists and the allowEmpty argument is true,
			 * the method returns the null string, if allowEmpty is false it
			 * returns a IllegalArgumentException
			 */

			String startElt = "." + elt;
			int startEltIndex = text.indexOf(startElt);
			if (startEltIndex < 0) {
				if (allowEmpty)
					return "";
				throw new IllegalArgumentException("no start, elt=" + elt
						+ " text=" + text);
			}
			int start = startEltIndex + startElt.length();
			String endElt = "\n.";
			int end = text.indexOf(endElt, start);
			if (end < 0)
				throw new IllegalArgumentException("no end, elt=" + elt
						+ " text=" + text);
			return text.substring(start, end);
		}
	}

	public static class Reuters21578Parser {

		/*
		 * Project testlucene 3.6.0, the Reuters21578Parser class parses the
		 * collection.
		 */

		private static final String END_BOILERPLATE_1 = "Reuter\n&#3;";
		private static final String END_BOILERPLATE_2 = "REUTER\n&#3;";

		// private static final String[] TOPICS = { "acq", "alum", "austdlr",
		// "barley", "bean", "belly", "bfr", "bop", "cake", "can", "carcass",
		// "castor", "castorseed", "cattle", "chem", "citruspulp", "cocoa",
		// "coconut", "coffee", "copper", "copra", "corn", "cornglutenfeed",
		// "cotton", "cottonseed", "cpi", "cpu", "crude", "cruzado", "debt",
		// "dfl", "dkr", "dlr", "dmk", "earn", "f", "feed", "fishmeal",
		// "fuel", "fx", "gas", "gnp", "gold", "grain", "groundnut", "heat",
		// "hk", "hog", "housing", "income", "instal", "interest",
		// "inventories", "ipi", "iron", "jet", "jobs", "l", "lead", "lei",
		// "lin", "linseed", "lit", "livestock", "lumber", "meal", "metal",
		// "money", "naphtha", "nat", "nickel", "nkr", "nzdlr", "oat", "oil",
		// "oilseed", "orange", "palladium", "palm", "palmkernel", "peseta",
		// "pet", "platinum", "plywood", "pork", "potato", "propane", "rand",
		// "rape", "rapeseed", "red", "reserves", "retail", "rice", "ringgit",
		// "rubber", "rupiah", "rye", "saudriyal", "sfr", "ship", "silver",
		// "skr", "sorghum", "soy", "soybean", "steel", "stg", "strategic",
		// "sugar", "sun", "sunseed", "supply", "tapioca", "tea", "tin",
		// "trade", "veg", "wheat", "wool", "wpi", "yen", "zinc" };

		public static List<List<String>> parseString(StringBuffer fileContent) {
			/* First the contents are converted to a string */
			String text = fileContent.toString();

			/*
			 * The method split of the String class splits the strings using the
			 * delimiter which was passed as argument Therefor lines is an array
			 * of strings, one string for each line
			 */
			String[] lines = text.split("\n");

			/*
			 * For each Reuters article the parser returns a list of strings
			 * where each element of the list is a field (TITLE, BODY, TOPICS,
			 * DATELINE). Each *.sgm file that is passed in fileContent can
			 * contain many Reuters articles, so finally the parser returns a
			 * list of list of strings, i.e, a list of reuters articles, that is
			 * what the object documents contains
			 */

			List<List<String>> documents = new LinkedList<List<String>>();

			/* The tag REUTERS identifies the beginning and end of each article */

			for (int i = 0; i < lines.length; ++i) {
				if (!lines[i].startsWith("<REUTERS"))
					continue;
				StringBuilder sb = new StringBuilder();
				while (!lines[i].startsWith("</REUTERS")) {
					sb.append(lines[i++]);
					sb.append("\n");
				}
				/*
				 * Here the sb object of the StringBuilder class contains the
				 * Reuters article which is converted to text and passed to the
				 * handle document method that will return the document in the
				 * form of a list of fields
				 */
				documents.add(handleDocument(sb.toString()));
			}
			return documents;
		}

		public static List<String> handleDocument(String text) {

			/*
			 * This method returns the Reuters article that is passed as text as
			 * a list of fields
			 */

			/* The fields TOPICS, TITLE, DATELINE and BODY are extracted */
			/* Each topic inside TOPICS is identified with a tag D */
			/* If the BODY ends with boiler plate text, this text is removed */

			String topics = extract("TOPICS", text, true);
			String title = extract("TITLE", text, true);
			String dateline = extract("DATELINE", text, true);
			String body = extract("BODY", text, true);
			String date = extract("DATE", text, true);

			if (body.endsWith(END_BOILERPLATE_1)
					|| body.endsWith(END_BOILERPLATE_2))
				body = body.substring(0,
						body.length() - END_BOILERPLATE_1.length());
			List<String> document = new LinkedList<String>();
			document.add(title);
			document.add(body);
			document.add(topics.replaceAll("\\<D\\>", " ").replaceAll(
					"\\<\\/D\\>", ""));
			document.add(dateline);
			document.add(date);
			return document;
		}

		private static String extract(String elt, String text,
				boolean allowEmpty) {

			/*
			 * This method find the tags for the field elt in the String text
			 * and extracts and returns the content
			 */
			/*
			 * If the tag does not exists and the allowEmpty argument is true,
			 * the method returns the null string, if allowEmpty is false it
			 * returns a IllegalArgumentException
			 */

			String startElt = "<" + elt + ">";
			String endElt = "</" + elt + ">";
			int startEltIndex = text.indexOf(startElt);
			if (startEltIndex < 0) {
				if (allowEmpty)
					return "";
				throw new IllegalArgumentException("no start, elt=" + elt
						+ " text=" + text);
			}
			int start = startEltIndex + startElt.length();
			int end = text.indexOf(endElt, start);
			if (end < 0)
				throw new IllegalArgumentException("no end, elt=" + elt
						+ " text=" + text);
			return text.substring(start, end);
		}
	}

	static class WorkerThread implements Runnable {

		private final Path folder;
		private final IndexWriter writer;
		private final boolean route;
		private final int fileindex;
		private final boolean removedups;
		private final String index;
		private final String parsername;

		public WorkerThread(final Path folder, IndexWriter writer,
				boolean route, int sgm, boolean removedups, String index,
				String parsername) {
			this.folder = folder;
			this.writer = writer;
			this.route = route;
			this.fileindex = sgm;
			this.removedups = removedups;
			this.index = index;
			this.parsername = parsername;
		}

		/**
		 * This is the work that the current thread will do when processed by
		 * the pool. In this case, it will only print some information.
		 */
		public void run() {
			final File docDir = new File(folder.toString());
			if (!docDir.exists() || !docDir.canRead()) {
				System.out
						.println("Document directory '"
								+ docDir.getAbsolutePath()
								+ "' does not exist or is not readable, please check the path");
				System.exit(1);
			}

			try {
				System.out.println(String.format("Thread '%s' Indexing '%s'",
						Thread.currentThread().getName(), folder));
				indexDocs(writer, docDir, route, fileindex, parsername);

			} catch (IOException e) {
				System.out.println(" caught a " + e.getClass()
						+ "\n with message: " + e.getMessage());
			}

		}
	}

	//Comprueba que el nombre de archivo se ajuste a la expresion reguñlar
	static boolean checkFile(String fnam, String parser) {
		//System.err.println(fnam + " ; "+ parser + " ; " );
		
		if ("cacm".equals(parser)){
			//return fnam.matches("cacm.all");					
			return fnam.matches("cacm-\\d\\d\\d.all");}
		else
			return fnam.matches("reut2-\\d\\d\\d.sgm");
	}

	static boolean checkFile(String fnam, String parser,int m) {
		int n=-1;
		if (checkFile(fnam, parser)) 
		{	
			if(m==-1)return true;
			if("cacm".equals(parser))
				n = Integer.parseInt(fnam.substring(5, 8));
			else
				n = Integer.parseInt(fnam.substring(6, 9));
			return (n == m);
		}
		else	
			return false;
	}

	static String dateConversion(String rawDate) throws ParseException {
		SimpleDateFormat dt = new SimpleDateFormat("d-MMM-yyyy hh:mm:ss");
		Date date = null;
		date = dt.parse(rawDate.trim());
		return DateTools.dateToString(date, DateTools.Resolution.SECOND);
	}

	static void indexDocs(IndexWriter writer, File file, boolean route,
			int fileIndex, String parsername) throws IOException {
		// do not try to index files that cannot be read
		if (file.canRead()) {
			if (file.isDirectory()) {
				String[] files = file.list();
				// an IO error could occur
				if (files != null) {
					for (int i = 0; i < files.length; i++) {

						indexDocs(writer, new File(file, files[i]), route, fileIndex,
								parsername);

					}
				}
			} else if (checkFile(file.getName(), parsername,fileIndex)) {
				System.out.println("Indexing file: " + file.getName() + " parser: " + parsername); 
				FileInputStream fis;
				try {
					fis = new FileInputStream(file);
				} catch (FileNotFoundException fnfe) {
					// at least on windows, some temporary files raise this
					// exception with an "access denied" message
					// checking if the file can be read doesn't help
					return;
				}

				try {

					byte[] encoded = Files.readAllBytes(file.toPath());
					String text = new String(encoded, StandardCharsets.UTF_8);

					char[] content = text.toCharArray();
					StringBuffer buffer = new StringBuffer();
					buffer.append(content);
					List<List<String>> dataList = new LinkedList<List<String>>();
					if ("cacm".equals(parsername)) {
						dataList = CACMParser.parseString(buffer);
						for (List<String> articulo : dataList) {

							Document doc = new Document();

							doc.add(new TextField("docid", articulo.get(0),
									Field.Store.YES));
							doc.add(new TextField("title", articulo.get(1),
									Field.Store.YES));
							doc.add(new TextField("abstract", articulo.get(2),
									Field.Store.NO));
							doc.add(new TextField("date", articulo.get(3),
									Field.Store.YES));
							doc.add(new TextField("authors", articulo.get(4),
									Field.Store.YES));
							doc.add(new TextField("keywords", articulo.get(5),
									Field.Store.YES));
							doc.add(new TextField("entrydate", articulo.get(6),
									Field.Store.NO));
							doc.add(new TextField("hash", Integer.toString(articulo.toString().hashCode()),
									Field.Store.YES));
							if (route)
								doc.add(new StringField("path", file.getPath(),
										Field.Store.YES));
							writer.addDocument(doc);

						}
					} else {
						dataList = Reuters21578Parser.parseString(buffer);
						for (List<String> articulo : dataList) {

							Document doc = new Document();
							doc.add(new TextField("title", articulo.get(0),
									Field.Store.YES));
							doc.add(new TextField("topics", articulo.get(2),
									Field.Store.YES));
							doc.add(new TextField("body", articulo.get(1),
									Field.Store.NO));
							doc.add(new TextField("dateline", articulo.get(3),
									Field.Store.NO));
							doc.add(new TextField("hash", Integer.toString(articulo.toString().hashCode()),
									Field.Store.YES));
							if (route)
								doc.add(new StringField("path", file.getPath(),
										Field.Store.YES));
							writer.addDocument(doc);
						}
					}

					/*
					 * 
					 * 
					 * doc.add(new TextField("contents", new BufferedReader( new
					 * InputStreamReader(fis, StandardCharsets.UTF_8))));
					 */
				} finally {
					fis.close();
				}
			}
		}
	}

	static void removeDups(IndexWriter writer)
			throws IOException {
		Directory dir = writer.getDirectory();
		dir.clearLock("write");
		String hash ="";		
		DirectoryReader dreader = DirectoryReader.open(dir);
		AtomicReader atomicReader = SlowCompositeReaderWrapper
				. wrap (( CompositeReader )  dreader ) ;
		for(int i=0;i<atomicReader.getDocCount("hash");i++)
		{
			hash=atomicReader.document(i).get("hash");
			System.err.println(hash);
		}
		
	}

	/** Index all text files under a directory. */
	public static void main(String[] args) {
		String usage = "es.udc.fic.ri.P1.ej2.ConcurrentIndexator\n"
				+ "[-openmode OPENMODE] [-parser PARSER] "
				+ "[-index INDEX_PATH] [-coll PATHNAME] "
				+ "[-onlydocs INT] [-addroute] " + "[-indexes PATHNAME(1..n)] "
				+ "[-colls PATHNAME(1..n)] [-removedups] \n\n";

		List<Path> indexPath = new ArrayList<Path>();
		List<Path> docsPath = new ArrayList<Path>();
		OpenMode mode = OpenMode.CREATE_OR_APPEND;
		boolean route = false;
		boolean removedups = false;
		int sgm = -1;
		int nDirs = 0;
		String parsername = "";

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath.add(Paths.get(args[i + 1]));
				i++;
			} else if ("-coll".equals(args[i])) {
				docsPath.add(Paths.get(args[i + 1]));
				i++;
			} else if ("-openmode".equals(args[i])) {
				if (args[i + 1].equals("append")) {
					mode = OpenMode.APPEND;

				} else if (args[i + 1].equals("create")) {
					mode = OpenMode.CREATE;
				} else if (args[i + 1].equals("create_or_append")) {
				} else {
					System.err.println("Usage: " + usage);
					System.exit(1);
				}
				i++;
			} else if ("-indexes".equals(args[i])) {
				while ((i + 1) < args.length
						&& Files.isDirectory(Paths.get(args[i + 1]))) {
					indexPath.add(Paths.get(args[i + 1]));
					i++;
				}
			} else if ("-colls".equals(args[i])) {
				while ((i + 1) < args.length
						&& Files.isDirectory(Paths.get(args[i + 1]))) {
					docsPath.add(Paths.get(args[i + 1]));
					i++;
				}
			} else if ("-addroute".equals(args[i])) {
				route = true;

			} else if ("-removedups".equals(args[i])) {
				removedups = true;

			} else if ("-onlydocs".equals(args[i])) {
				sgm = Integer.parseInt(args[i + 1]);
			} else if ("-parser".equals(args[i])) {
				parsername = args[i + 1];
				if(!("cacm".equals(parsername)||"reuters".equals(parsername)))
				{	
					System.err.println("Usage: " + usage);
					System.exit(1);
				}	
			}

		}

		if (docsPath.isEmpty() || docsPath.size() != indexPath.size()) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		nDirs = indexPath.size();

		final int numCores = Runtime.getRuntime().availableProcessors();
		final ExecutorService executor = Executors.newFixedThreadPool(numCores);
		List<IndexWriter> writer = new ArrayList<IndexWriter>();
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
		List<IndexWriterConfig> iwc = new ArrayList<IndexWriterConfig>();

		try {
			for (int i = 0; i < nDirs; i++) {
				iwc.add(new IndexWriterConfig(Version.LUCENE_4_10_0, analyzer));
				iwc.get(i).setOpenMode(mode);
				Directory dir = FSDirectory.open(new File(indexPath.get(i)
						.toString()));
				writer.add(new IndexWriter(dir, iwc.get(i)));

				if (Files.isDirectory(docsPath.get(i))) {
					Path path = docsPath.get(i);
					final Runnable worker = new WorkerThread(path,
							writer.get(i), route, sgm, removedups, indexPath
									.get(i).toString(), parsername);
					executor.execute(worker);
				}
			}
			/*
			 * Close the ThreadPool; no more jobs will be accepted, but all the
			 * previously submitted jobs will be processed.
			 */
			executor.shutdown();

			/*
			 * Wait up to 1 hour to finish all the previously submitted jobs and
			 * close IndexWriter
			 */

			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(-2);
		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(-2);
		} finally {
			System.out.println("Finished all indexations");
			System.out.print("Merging indexes into " + indexPath.get(0).toString() + "...");
			try {
				for (int i = 1; i < nDirs; i++) {
					Directory dir = writer.get(i).getDirectory();
					writer.get(i).close();
					writer.get(0).addIndexes(dir);
				}
				removeDups(writer.get(0));
				writer.get(0).close();
				System.out.print(" DONE");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}


