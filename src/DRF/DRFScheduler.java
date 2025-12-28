package DRF;

import java.util.*;


public class DRFScheduler {
    private final double[] totalResources;
    private final double[] remainingResources;
    private final List<User> users;

    public DRFScheduler(double[] totalResources, List<User> users) {
        this.totalResources = totalResources;
        this.remainingResources = totalResources.clone();
        this.users = users;
    }

    public void schedule() {
        Set<User> activeUsers = new HashSet<>(users);

        while (!activeUsers.isEmpty()) {
            User minUser = null;
            double minShare = Double.MAX_VALUE;

            // Find user with smallest dominant share
            for (User u : activeUsers) {
                double share = u.dominantShare(totalResources);
                if (share < minShare) {
                    minShare = share;
                    minUser = u;
                }
            }

            if (minUser == null || !fits(minUser.demand)) {
                activeUsers.remove(minUser);
                continue;
            }

            // Allocate one task
            allocate(minUser);
        }
    }

    private boolean fits(double[] demand) {
        for (int i = 0; i < demand.length; i++) {
            if (demand[i] > remainingResources[i]) {
                return false;
            }
        }
        return true;
    }

    private void allocate(User user) {
        for (int i = 0; i < user.demand.length; i++) {
            user.allocation[i] += user.demand[i];
            remainingResources[i] -= user.demand[i];
        }
        user.tasks++;
    }

    static class User {
        final String id;
        final double[] demand;      // per-task demand
        final double[] allocation;  // total allocated resources
        int tasks;

        User(String id, double[] demand, int resourceCount) {
            this.id = id;
            this.demand = demand;
            this.allocation = new double[resourceCount];
            this.tasks = 0;
        }

        double dominantShare(double[] totalResources) {
            double max = 0.0;
            for (int i = 0; i < allocation.length; i++) {
                max = Math.max(max, allocation[i] / totalResources[i]);
            }
            return max;
        }
    }

    public static void main(String[] args) {
        double[] total = {100, 100}; // CPU, MEM

        User u1 = new User("A", new double[]{1, 4}, 2);
        User u2 = new User("B", new double[]{3, 1}, 2);

        DRFScheduler scheduler = new DRFScheduler(
                total,
                List.of(u1, u2)
        );

        scheduler.schedule();

        System.out.println(u1.id + " tasks: " + u1.tasks);
        System.out.println(u2.id + " tasks: " + u2.tasks);
    }
}
