package org.deri.tarql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.deri.tarql.csv.CSVFormat;
import org.deri.tarql.csv.CSVFormat.ParseResult;
import org.junit.Test;

public class CSVFragmentTest {
	private final static String absoluteNoFragment = "http://example.com/file.csv";

	@Test
	public void testNoFragment() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment);
		assertEquals(absoluteNoFragment, parsed.getRemainingIRI());
		assertNull(parsed.getOptions().getEncoding());
		assertNull(parsed.getOptions().hasColumnNamesInFirstRow());
	}

	@Test
	public void testEmptyFragment() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#");
		assertEquals(absoluteNoFragment + "#", parsed.getRemainingIRI());
		assertNull(parsed.getOptions().getEncoding());
		assertNull(parsed.getOptions().hasColumnNamesInFirstRow());
	}

	@Test
	public void testEmptyRelativeFragment() {
		ParseResult parsed = CSVFormat.parseIRI("#");
		assertEquals("#", parsed.getRemainingIRI());
		assertNull(parsed.getOptions().getEncoding());
		assertNull(parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testRetainNonTarqlFragment() {
		ParseResult parsed = CSVFormat.parseIRI("#foo");
		assertEquals("#foo", parsed.getRemainingIRI());
		assertNull(parsed.getOptions().getEncoding());
		assertNull(parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testExtractEncoding() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#encoding=utf-8");
		assertEquals(absoluteNoFragment, parsed.getRemainingIRI());
		assertEquals("utf-8", parsed.getOptions().getEncoding());
	}
	
	@Test
	public void testExtractCharset() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#charset=utf-8");
		assertEquals(absoluteNoFragment, parsed.getRemainingIRI());
		assertEquals("utf-8", parsed.getOptions().getEncoding());
	}
	
	@Test
	public void testExtractPresentHeader() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#header=present");
		assertEquals(absoluteNoFragment, parsed.getRemainingIRI());
		assertEquals(true, parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testExtractAbsentHeader() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#header=absent");
		assertEquals(absoluteNoFragment, parsed.getRemainingIRI());
		assertEquals(false, parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testIgnoreUnrecognizedHeaderValue() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#header=foo");
		assertEquals(absoluteNoFragment + "#header=foo", parsed.getRemainingIRI());
		assertNull(parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testExtractMultipleKeys() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#encoding=utf-8;header=absent");
		assertEquals(absoluteNoFragment, parsed.getRemainingIRI());
		assertEquals("utf-8", parsed.getOptions().getEncoding());
		assertEquals(false, parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testRetainUnknownKeys() {
		ParseResult parsed = CSVFormat.parseIRI(absoluteNoFragment + "#encoding=utf-8;foo=bar;header=absent");
		assertEquals(absoluteNoFragment + "#foo=bar", parsed.getRemainingIRI());
		assertEquals("utf-8", parsed.getOptions().getEncoding());
		assertEquals(false, parsed.getOptions().hasColumnNamesInFirstRow());
	}
	
	@Test
	public void testExtractDelimiter() {
		assertNull(CSVFormat.parseIRI("file.csv").getOptions().getDelimiter());
		assertEquals(',', CSVFormat.parseIRI("file.csv#delimiter=,").getOptions().getDelimiter().charValue());
		assertEquals(',', CSVFormat.parseIRI("file.csv#delimiter=comma").getOptions().getDelimiter().charValue());
		assertEquals(';', CSVFormat.parseIRI("file.csv#delimiter=semicolon").getOptions().getDelimiter().charValue());
		assertEquals('\t', CSVFormat.parseIRI("file.csv#delimiter=tab").getOptions().getDelimiter().charValue());
		assertEquals(' ', CSVFormat.parseIRI("file.csv#delimiter=%20").getOptions().getDelimiter().charValue());
		assertEquals(';', CSVFormat.parseIRI("file.csv#delimiter=%3B").getOptions().getDelimiter().charValue());
		assertEquals(',', CSVFormat.parseIRI("file.csv#delimiter=%2C").getOptions().getDelimiter().charValue());
		assertEquals('\t', CSVFormat.parseIRI("file.csv#delimiter=%09").getOptions().getDelimiter().charValue());
		assertNull(CSVFormat.parseIRI("file.csv#delimiter=foo").getOptions().getDelimiter());
	}
	
	@Test
	public void testExtractQuoteChar() {
		assertNull(CSVFormat.parseIRI("file.csv").getOptions().getQuoteChar());
		assertEquals('"', CSVFormat.parseIRI("file.csv#quotechar=%22").getOptions().getQuoteChar().charValue());
		assertEquals('"', CSVFormat.parseIRI("file.csv#quotechar=doublequote").getOptions().getQuoteChar().charValue());
		assertEquals('\'', CSVFormat.parseIRI("file.csv#quotechar=%27").getOptions().getQuoteChar().charValue());
		assertEquals('\'', CSVFormat.parseIRI("file.csv#quotechar=singlequote").getOptions().getQuoteChar().charValue());
		assertNull(CSVFormat.parseIRI("file.csv#quotechar=foo").getOptions().getDelimiter());
	}
	
	@Test
	public void testExtractEscapeChar() {
		assertNull(CSVFormat.parseIRI("file.csv").getOptions().getEscapeChar());
		assertEquals('\\', CSVFormat.parseIRI("file.csv#escapechar=%5C").getOptions().getEscapeChar().charValue());
		assertEquals('\\', CSVFormat.parseIRI("file.csv#escapechar=backslash").getOptions().getEscapeChar().charValue());
		assertNull(CSVFormat.parseIRI("file.csv#escapechar=foo").getOptions().getEscapeChar());
	}
}
