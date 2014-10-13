package org.deri.tarql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.deri.tarql.csv.CSVFormat;

import com.hp.hpl.jena.sparql.algebra.Table;
import com.hp.hpl.jena.sparql.algebra.table.TableBase;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import com.hp.hpl.jena.util.iterator.ClosableIterator;

/**
 * Implementation of ARQ's {@link Table} interface over a tabular file.
 * Supports opening multiple iterators over the input, which
 * will lead to multiple passes over the input file if streaming
 * is supported for the file format.
 * Connects to the input as lazily as possible, while still
 * supporting the entire Table interface including {@link #size()}.
 */
public class TarqlTable extends TableBase implements Table {
	private final InputStreamSource source;
	private final TableFormat format;
	private final List<ClosableIterator<Binding>> openIterators = new ArrayList<ClosableIterator<Binding>>();
	private ClosableIterator<Binding> nextParser = null;
	private List<Var> varsCache = null;
	private Boolean isEmptyCache = null;
	private Integer sizeCache = null;
	
	public TarqlTable(InputStreamSource source) {
		this(source, new CSVFormat());
	}

	public TarqlTable(InputStreamSource source, TableFormat format) {
		this.source = source;
		this.format = format;
	}
	
	@Override
	public QueryIterator iterator(ExecutionContext ctxt) {
		// QueryIteratorPlainWrapper doesn't close wrapped 
		// ClosableIterators, so we do that ourselves.
		final ClosableIterator<Binding> wrapped = rows();
		return new QueryIterPlainWrapper(wrapped, ctxt) {
			@Override
			protected void closeIterator() {
				super.closeIterator();
				wrapped.close();
			}
		};
	}

	@Override
	public ClosableIterator<Binding> rows() {
		ensureHasParser();
		final ClosableIterator<Binding> wrappedIterator = nextParser;
		nextParser = null;
		// We will add a wrapper to the iterator that removes it
		// from the list of open iterators once it is closed and
		// exhausted, and that fills the size cache once the
		// iterator is exhausted.
		return new ClosableIterator<Binding>() {
			private int count = 0;
			@Override
			public boolean hasNext() {
				if (wrappedIterator.hasNext()) return true;
				if (sizeCache == null) sizeCache = count;
				openIterators.remove(wrappedIterator);
				return false;
			}
			@Override
			public Binding next() {
				count++;
				return wrappedIterator.next();
			}
			@Override
			public void remove() {
				wrappedIterator.remove();
			}
			@Override
			public void close() {
				openIterators.remove(wrappedIterator);
				wrappedIterator.close();
			}
		};
	}
	
	@Override
	public List<Var> getVars() {
		ensureHasParser();
		return varsCache;
	}
	
	@Override
	public List<String> getVarNames() {
		return Var.varNames(getVars());
	}

	/**
	 * Returns <code>true</code> if the table has zero rows.
	 * Is fast.
	 */
	@Override
	public boolean isEmpty() {
		ensureHasParser();
		return isEmptyCache;
	}

	/**
	 * Returns the number of rows in the table. Is fast if an iterator
	 * over the table has already been exhausted. Otherwise, it will
	 * make a complete parsing pass over the input.
	 */
	@Override
	public int size() {
		if (sizeCache == null) {
			// This fills the cache.
			Iterator<Binding> it = rows();
			while (it.hasNext()) it.next();
		}
		return sizeCache;
	}
	
	/**
	 * Closes any open iterators over the table.
	 */
	@Override
	public void closeTable() {
		while (!openIterators.isEmpty()) {
			ClosableIterator<Binding> next = openIterators.remove(0);
			next.close();
		}
	}
	
	private void ensureHasParser() {
		if (nextParser == null) {
			TableParser parser = createParser();
			if (varsCache == null) {
				varsCache = parser.getVars();
			}
			if (isEmptyCache == null) {
				isEmptyCache = !parser.hasNext();
			}
			nextParser = parser;
		}
	}
	
	private TableParser createParser() {
		try {
			TableParser result = format.openParserFor(source);
			openIterators.add(result);
			return result;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
}
