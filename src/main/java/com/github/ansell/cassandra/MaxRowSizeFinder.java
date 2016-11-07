package com.github.ansell.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Finds the maximum row size in a Cassandra database.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public final class MaxRowSizeFinder {

	/**
	 * Private constructor for static only class
	 */
	private MaxRowSizeFinder() {
	}

	public static void main(String... args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<String> database = parser.accepts("database").withRequiredArg().ofType(String.class).required()
				.describedAs("The Cassandra database to query.");

		OptionSet options = null;

		try {
			options = parser.parse(args);
		} catch (final OptionException e) {
			System.out.println(e.getMessage());
			parser.printHelpOn(System.out);
			throw e;
		}

		if (options.has(help)) {
			parser.printHelpOn(System.out);
			return;
		}

		try (Cluster cluster = Cluster.builder() // (1)
				.addContactPoint("127.0.0.1").build();) {
			Session session = cluster.connect(); // (2)

			ResultSet rs = session.execute("select release_version from system.local"); // (3)
			Row row = rs.one();
			System.out.println(row.getString("release_version")); // (4)
		}

	}

}
