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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConcurrentIndexator {

	static class WorkerThread implements Runnable {

		private final Path folder;
		private final IndexWriter writer;

		public WorkerThread(final Path folder, IndexWriter writer) {
			this.folder = folder;
			this.writer = writer;
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

				indexDocs(writer, docDir);

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

	static void indexDocs(IndexWriter writer, File file) throws IOException {
		// do not try to index files that cannot be read
		if (file.canRead()) {
			if (file.isDirectory()) {
				String[] files = file.list();
				// an IO error could occur
				if (files != null) {
					for (int i = 0; i < files.length; i++) {
						indexDocs(writer, new File(file, files[i]));
					}
				}
			} else {

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
					Field pathField = new StringField("file", file.getName(),
							Field.Store.YES);
					doc.add(pathField);

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
					doc.add(new LongField("modified", file.lastModified(),
							Field.Store.NO));

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
		String usage = "java org.apache.lucene.demo.IndexFiles"
				+ " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		Path indexPath = null;
		DirectoryStream<Path> docsPath = null;
		OpenMode mode = OpenMode.CREATE_OR_APPEND;
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = Paths.get(args[i + 1]);
				i++;
			} else if ("-coll".equals(args[i])) {
				try {
					docsPath = Files.newDirectoryStream(Paths.get(args[i + 1]));
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(-1);
				}
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
			}
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		final int numCores = Runtime.getRuntime().availableProcessors();
		final ExecutorService executor = Executors.newFixedThreadPool(numCores);
		IndexWriter writer = null;

		try {
			Directory dir = FSDirectory.open(new File(indexPath.toString()));
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_0);
			IndexWriterConfig iwc = new IndexWriterConfig(
					Version.LUCENE_4_10_0, analyzer);

			iwc.setOpenMode(mode);

			writer = new IndexWriter(dir, iwc);

			for (final Path path : docsPath) {
				if (Files.isDirectory(path)) {
					final Runnable worker = new WorkerThread(path, writer);
					/*
					 * Send the thread to the ThreadPool. It will be processed
					 * eventually.
					 */
					executor.execute(worker);
				} else if (Files.isRegularFile(path)) {
					/* Files not contained in subfolders */
					indexDocs(writer, path.toFile());
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
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
		try {
			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(-2);
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Finished all threads");

	}
}
