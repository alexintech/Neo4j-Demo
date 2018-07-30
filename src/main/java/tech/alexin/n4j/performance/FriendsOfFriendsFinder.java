package tech.alexin.n4j.performance;

public interface FriendsOfFriendsFinder {

	Long countFriendsOfFriends(Long userId, int depth);

	Long shortestPathBetween2Users(Long userId1, Long userId2);
}
