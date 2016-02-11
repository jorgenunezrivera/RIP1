package es.udc.fic.ri.P1.ej2;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrentIndexator {

	static class WorkerThread implements Runnable {

		private final Path folder;
		private final IndexWriter writer;
		private final boolean route;
		private final int sgm;

		public WorkerThread(final Path folder, IndexWriter writer,
				boolean route, int sgm) {
			this.folder = folder;
			this.writer = writer;
			this.route = route;
			this.sgm = sgm;
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

				indexDocs(writer, docDir, route, sgm);

				// NOTE: if you want to maximize search performance,
				// you can optionally call forceMerge here. This can be
				// a terribly costly operation, so generally it's only
				// worth it when your index is relatively static (ie
				// you're done adding documents to it):
				//
				// writer.forceMerge(1);

			} catch (IOException e) {
				System.out.println(" caught a " + e.getClass()
						+ "\n with message: " + e.getMessage());
			}

		}
	}

	static boolean checkFile(String fnam) {
		return fnam.matches("reut2-\\d\\d\\d.sgm");
	}

	static boolean checkFile(String fnam, int m) {
		if (checkFile(fnam)) {
			int n = Integer.parseInt(fnam.substring(6, 9));
			return (n == m);
		} else
			return false;
	}

	static void indexDocs(IndexWriter writer, File file, boolean route, int sgm)
			throws IOException {
		// do not try to index files that cannot be read
		if (file.canRead()) {
			if (file.isDirectory()) {
				String[] files = file.list();
				// an IO error could occur
				if (files != null) {
					for (int i = 0; i < files.length; i++) {
						indexDocs(writer, new File(file, files[i]), route, sgm);
					}
				}
			} else if (checkFile(file.getName())
					&& (sgm == -1 || checkFile(file.getName(), sgm))) {

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

					// make a new, empty document
					Document doc = new Document();

					// Add the path of the file as a field named "path". Use
					// a
					// field that is indexed (i.e. searchable), but don't
					// tokenize
					// the field into separate words and don't index term
					// frequency
					// or positional information:
					// Add the last modified date of the file a field named
					// "modified".
					// Use a LongField that is indexed (i.e. efficiently
					// filterable with
					// NumericRangeFilter). This indexes to milli-second
					// resolution, which
					// is often too fine. You could instead create a number
					// based on
					// year/month/day/hour/minutes/seconds, down the
					// resolution
					// you require.
					// For example the long value 2011021714 would mean
					// February 17, 2011, 2-3 PM.
					if (route) {
						Field pathField = new StringField("path",
								file.getPath(), Field.Store.YES);
						doc.add(pathField);
					}

					byte[] encoded = Files.readAllBytes(file.toPath());
					String text = new String(encoded, StandardCharsets.UTF_8);
					char[] content = text.toCharArray();
					StringBuffer buffer = new StringBuffer();
					buffer.append(content);
					List<List<String>> dataList = Reuters21578Parser
							.parseString(buffer);

					StringBuffer topics = new StringBuffer();
					for (List<String> list : dataList) {
						topics.append(list.get(2) + " ");
					}

					StringBuffer body = new StringBuffer();
					for (List<String> list : dataList) {
						body.append(list.get(1) + " ");
					}

					StringBuffer title = new StringBuffer();
					for (List<String> list : dataList) {
						title.append(list.get(0) + " ");
					}

					StringBuffer dateline = new StringBuffer();
					for (List<String> list : dataList) {
						dateline.append(list.get(3) + " ");
					}

					StringBuffer date = new StringBuffer();
					for (List<String> list : dataList) {
						date.append(list.get(4) + " ");
					}

					doc.add(new TextField("topics", topics.toString(),
							Field.Store.NO));

					doc.add(new TextField("body", body.toString(),
							Field.Store.NO));

					doc.add(new TextField("title", title.toString(),
							Field.Store.YES));

					doc.add(new TextField("dateline", dateline.toString(),
							Field.Store.NO));

					doc.add(new TextField("date", date.toString(),
							Field.Store.YES));

					if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
						// New index, so we just add the document (no old
						// document can be there):
						System.out.println("adding " + file);
						writer.addDocument(doc);
					} else {
						// Existing index (an old copy of this document may
						// have
						// been indexed) so
						// we use updateDocument instead to replace the old
						// one
						// matching the exact
						// path, if present:
						System.out.println("updating " + file);
						writer.updateDocument(new Term("path", file.getPath()),
								doc);
					}

				} finally {
					fis.close();
				}
			}
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
		int sgm = -1;
		int nDirs = 0;

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
			} else if ("-onlydocs".equals(args[i])) {
				sgm = Integer.parseInt(args[i + 1]);
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
							writer.get(i), route, sgm);
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
			try {
				for (int i = 0; i < nDirs; i++)
					writer.get(i).close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Finished all threads");

	}
}
