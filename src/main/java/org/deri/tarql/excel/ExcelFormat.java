package org.deri.tarql.excel;

import java.io.IOException;
import java.io.Reader;

import org.deri.tarql.InputStreamSource;
import org.deri.tarql.TableFormat;
import org.deri.tarql.TableParser;

public class ExcelFormat implements TableFormat {

	@Override
	public boolean supportsStreaming() {
		return false;
	}

	@Override
	public Boolean hasColumnNamesInFirstRow() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableParser openParserFor(InputStreamSource source)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader openReaderFor(InputStreamSource source) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
