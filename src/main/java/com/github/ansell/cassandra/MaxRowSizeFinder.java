package com.github.ansell.cassandra;

import java.util.ArrayList;
import java.util.List;

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

		try (Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();) {
			Session session = cluster.connect();

			ResultSet versionRs = session.execute("select release_version from system.local");
			Row versionRow = versionRs.one();

			String version = versionRow.getString("release_version");
			System.out.println("Cassandra host reports its version as: " + version);

			if (version.startsWith("1.")) {
				System.out.println(
						"Warning: Cassandra-1.x does not support paging, all results will be fetched at one time.");
			}

			List<String> columnFamilies = new ArrayList<>();

			ResultSet columnsRs = session.execute("select columnfamily_name from system.schema_columnfamilies");
			for (Row nextColumnFamilyRow : columnsRs) {
				String nextColumnFamily = nextColumnFamilyRow.getString("columnfamily_name");
				columnFamilies.add(nextColumnFamily);
				System.out.println(nextColumnFamily);
			}

			for (String nextColumnFamily : columnFamilies) {
				ResultSet nextColumnRow = session.execute("select * from " + nextColumnFamily);
				int count = 0;
				for (Row nextRow : nextColumnRow) {
					count++;
				}
				System.out.println(nextColumnFamily + " count=" + count);
			}

		}

	}

}
