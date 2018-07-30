package tech.alexin.n4j.performance;

import org.apache.commons.lang3.RandomStringUtils;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Neo4jDataGenerator implements DataGenerator {
	private static Logger logger = LoggerFactory.getLogger(Neo4jDataGenerator.class);
	public static final int BATCH_SIZE = 10_000;
	public static final boolean USE_BATCH = true;

	private final Session session;
	private final List<Long> nodeIds = new ArrayList<>();

	public Neo4jDataGenerator(Session session) {
		this.session = session;
	}

	@Override
	public void generateUsers(int count, int friensPerUser) {
		StopWatch timer = new StopWatch();
		timer.start("Create " + count + " persons");

		Transaction tx = session.beginTransaction();
		try {
			for (long i = 1; i <= count; i++) {
				tx.run("CREATE (p:Person {name: {name}, userId: {userId}})",
						Values.parameters("name", RandomStringUtils.randomAlphabetic(8), "userId", i));
				nodeIds.add(i);
				if (USE_BATCH)
					if (i % BATCH_SIZE == 0) {
						tx.success();
						tx.close();
						tx = session.beginTransaction();
						logger.info("Commited batch: " + i + " personsa");
					}
			}
			logger.info("Commited batch");
			tx.success();
		} catch (Exception e) {
			logger.error(e.getMessage());
			tx.failure();
		} finally {
			tx.close();
		}

		timer.stop();  // End of creation of persons

		timer.start("Create " + friensPerUser + " relations per person");
		tx = session.beginTransaction();
		try {
			int relCnt = 0;
			for (long id : nodeIds) {
				for (int j = 0; j < friensPerUser; j++) {
					tx.run("MATCH (p:Person {userId: {userId}}) " +
									"MATCH (f:Person {userId: {friendId}}) " +
									"CREATE (p)-[r:IS_FRIEND_OF]->(f)",
							Values.parameters("userId", id, "friendId", getRandomFriendId()));
					relCnt++;
					if (USE_BATCH && relCnt % BATCH_SIZE == 0) {
						tx.success();
						tx.close();
						tx = session.beginTransaction();
						logger.info("Commited batch: " + relCnt + " relations");
					}
				}
			}
			logger.info("Commited batch");
			tx.success();
		} catch (Exception e) {
			logger.error(e.getMessage());
			tx.failure();
		} finally {
			tx.close();
		}
		timer.stop();
		System.out.println(timer.prettyPrint());
	}

	private long getRandomFriendId() {
		Random random = new Random();

		return Math.abs(random.nextInt(nodeIds.size()-1)) + 1;
	}
}
