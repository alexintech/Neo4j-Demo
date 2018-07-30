package tech.alexin.n4j.performance;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

public class Neo4jFriendsOfFriendsFinder implements FriendsOfFriendsFinder {

	private static Logger logger = LoggerFactory.getLogger(Neo4jFriendsOfFriendsFinder.class);

	private final Session session;

	public Neo4jFriendsOfFriendsFinder(Session session) {
		this.session = session;
	}

	@Override
	public Long countFriendsOfFriends(Long userId, int depth) {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		StatementResult statementResult =
				session.run("MATCH (p:Person {userId: {userId}})-[IS_FRIEND_OF*1.." + depth + "]-(friend) " +
					"RETURN count(distinct friend)",
					Values.parameters("userId", userId));

		Record record = statementResult.next();
		Long result = record.get("count(distinct friend)").asLong();

		stopWatch.stop();
		logger.info("NEO4J: Found {} friends of friends depth " + depth + " for user {}, took "
				+ stopWatch.getTotalTimeMillis() + " millis.", result, userId);

		return result;
	}

	@Override
	public Long shortestPathBetween2Users(Long userId1, Long userId2) {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();

		StatementResult statementResult =
				session.run("MATCH p=shortestPath((user1:Person {userId: {userId1} })-[*]-(user2:Person {userId: {userId2} }))" +
								" RETURN p",
						Values.parameters("userId", userId1, userId2));

		Record record = statementResult.next();
		Long result = record.get("p").asLong();

		stopWatch.stop();
		// logger.info("NEO4J: Found {} friends of friends depth " + depth + " for user {}, took "
		// 		+ stopWatch.getTotalTimeMillis() + " millis.", result, userId);

		return result;
	}
}
