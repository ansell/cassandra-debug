package com.github.ansell.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ansell.jdefaultdict.JDefaultDict;

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

			Map<String, List<String>> columnFamilies = new JDefaultDict<>(k -> new ArrayList<>());

			ResultSet columnsRs = session.execute("SELECT * FROM system.schema_columnfamilies");
			for (Row nextColumnFamilyRow : columnsRs) {
				String nextColumnFamily = nextColumnFamilyRow.getString("columnfamily_name");
				String nextKeyspace = nextColumnFamilyRow.getString("keyspace_name");
				columnFamilies.get(nextKeyspace).add(nextColumnFamily);
				System.out.println("");
				System.out.println(nextColumnFamily);
				System.out.println("");

				nextColumnFamilyRow.getColumnDefinitions().forEach(c -> {
					System.out.println("\t" + c.getName() + " => " + c.getType());
					// System.out.println(c.getName() + " => " +
					// nextColumnFamilyRow.getString(c.getName()));
				});
			}

			for (Entry<String, List<String>> nextColumnFamily : columnFamilies.entrySet()) {
				for (String nextColumnFamilyKey : nextColumnFamily.getValue()) {
					String nextQuery = "SELECT * FROM \"" + nextColumnFamily.getKey() + "\".\"" + nextColumnFamilyKey
							+ "\"";
					System.out.println(nextQuery);

					// ResultSet nextColumnRow = session.execute(nextQuery);
					// int count = 0;
					// for (Row nextRow : nextColumnRow) {
					// count++;
					// }
					// System.out.println(nextColumnFamily + " count=" + count);
				}
			}

		}

	}

}
