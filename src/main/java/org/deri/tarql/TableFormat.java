package org.deri.tarql;

import java.io.IOException;
import java.io.Reader;

public interface TableFormat {

	/**
	 * Indicates whether the {@link TableParser} returned by
	 * {@link #openParserFor(InputStreamSource source)} supports
	 * streaming, that is, can work with constant memory regardless
	 * of input file size
	 */
	boolean supportsStreaming();
	
	/**
	 * Set whether the CSV file's first row contains column names.
	 * <code>null</code> means unknown.
	 * The default is <code>null</code>.
	 */
	Boolean hasColumnNamesInFirstRow();

	/**
	 * Creates a new {@link TableParser} for a given {@link InputStreamSource}
	 * with the options of this instance.
	 */
	TableParser openParserFor(InputStreamSource source) throws IOException;
	
	/**
	 * Creates a new {@link Reader} for a given {@link InputStreamSource}
	 * with the options of this instance.
	 */
	Reader openReaderFor(InputStreamSource source) throws IOException;
}
