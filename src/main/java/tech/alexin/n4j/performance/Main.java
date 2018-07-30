package tech.alexin.n4j.performance;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StopWatch;

public class Main {
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	public static final int COUNT = 1_000_000;
	public static final int FRIENDS_PER_USER = 50;
	public static final int CACHE_WARMING_HITS = 10;

	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("classpath*:META-INF/spring/module-context.xml");

		// generateGraphData();

		runQuery(context);
	}

	private interface Executor {
		void execute();
	}

	private static void runQuery(ApplicationContext context) {
		try (Driver driver = GraphDatabase.driver("bolt://localhost",
				AuthTokens.basic("neo4j", "232323"));
		     Session session = driver.session()) {

			final FriendsOfFriendsFinder neo4jFriendsOfFriendsFinder = new Neo4jFriendsOfFriendsFinder(session);

			measureExecution(() -> neo4jFriendsOfFriendsFinder.countFriendsOfFriends(2L, 2));
			measureExecution(() -> neo4jFriendsOfFriendsFinder.countFriendsOfFriends(2L, 3));
			measureExecution(() -> neo4jFriendsOfFriendsFinder.countFriendsOfFriends(2L, 4));
			measureExecution(() -> neo4jFriendsOfFriendsFinder.countFriendsOfFriends(2L, 5));
		} catch (ClientException e) {
			System.err.println("Error occured: " + e.getMessage());
		}
	}

	private static void measureExecution(Executor executor) {
		long maxExecution = -1;
		long minExecution = -1;
		long totalExecutionTime = 0;
		for (int i = 0; i < CACHE_WARMING_HITS; i++) {
			StopWatch stopWatch = new StopWatch();
			stopWatch.start();
			executor.execute();
			stopWatch.stop();
			long executionTime = stopWatch.getTotalTimeMillis();
			if (minExecution < 0 || executionTime < minExecution) {
				minExecution = executionTime;
			}
			if (maxExecution < 0 || executionTime > maxExecution) {
				maxExecution = executionTime;
			}
			totalExecutionTime += executionTime;
		}

		double avg = totalExecutionTime / CACHE_WARMING_HITS;
		double withoutTop = (totalExecutionTime - maxExecution) / (CACHE_WARMING_HITS - 1);
		logger.info("Max time: {} millis, min time: {} millis.", maxExecution, minExecution);
		logger.info("Average: {} millis, 90-percetile: {} millis.", avg, withoutTop);
	}

	private static void generateGraphData() {

		try (Driver driver = GraphDatabase.driver("bolt://localhost",
				AuthTokens.basic("neo4j", "232323"));
		     Session session = driver.session()) {

			long elementCnt = 0;
			try (Transaction tx = session.beginTransaction()) {
				StatementResult result = tx.run("MATCH (p:Person) RETURN count(p)");
				Record record = result.next();
				elementCnt += record.get("count(p)").asLong();

				result = tx.run("MATCH ()-[r:IS_FRIEND_OF]->() RETURN count(r)");
				record = result.next();
				elementCnt += record.get("count(r)").asLong();
			}
			if (elementCnt != (COUNT + COUNT * FRIENDS_PER_USER)) {
				logger.info("Not enough users/friends ({}, required {}), deleting existing neo4j database nodes.",
						elementCnt, (COUNT + COUNT * FRIENDS_PER_USER));
				// try (Transaction tx = session.beginTransaction()) {
				// 	tx.run("MATCH (p:Person) OPTIONAL MATCH (p)-[r]-() DELETE p,r");
				// 	tx.success();
				// }

				session.run("CREATE INDEX ON :Person(userId)");
				// 	session.run("MATCH (p:Person) OPTIONAL MATCH (p)-[r]-() DELETE p,r");
				DataGenerator neo4jDataGenerator = new Neo4jDataGenerator(session);
				neo4jDataGenerator.generateUsers(COUNT, FRIENDS_PER_USER);
			}

		} catch (ClientException e) {
			System.err.println("Error occured: " + e.getMessage());
		}
	}
}
