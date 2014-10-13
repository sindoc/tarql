package org.deri.tarql;

import java.util.List;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.util.iterator.ClosableIterator;

public interface TableParser extends ClosableIterator<Binding> {

	List<Var> getVars();
}
