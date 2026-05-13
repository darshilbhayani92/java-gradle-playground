import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.stream.*;

// ─────────────────────────────────────────────────────────────────────────────
// PART 1 — DATA MODEL
// ─────────────────────────────────────────────────────────────────────────────

class Backup {
    final String id;
    final long   endTime;       // epoch ms — when the backup completed
    final long   retentionMs;   // how long to keep it after endTime
    final long   expiresAt;     // endTime + retentionMs

    Backup(String id, long endTime, long retentionMs) {
        this.id          = id;
        this.endTime     = endTime;
        this.retentionMs = retentionMs;
        this.expiresAt   = endTime + retentionMs;
    }

    boolean isExpired(long now) {
        return now > expiresAt;
    }

    @Override public String toString() {
        return String.format("Backup{id=%s, endTime=%d, expiresAt=%d}", id, endTime, expiresAt);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PART 2 — SINGLE-THREADED VERSION
// ─────────────────────────────────────────────────────────────────────────────
//
// Dependency model:
//   child → parent means "child depends on parent"
//   i.e. to restore child, you must also have parent
//   Example: incremental backup B2 depends on full backup B1
//            incremental backup B3 depends on B2
//            recovery chain for B3 = [B1, B2, B3]
//
// Graph structure:
//   dependencies : child  → parent  (one parent per child, like a tree)
//   dependents   : parent → [children] (reverse map, for expiry propagation)

class BackupSystem {

    // id → Backup object
    private final Map<String, Backup> backups = new HashMap<>();

    // child id → parent id
    private final Map<String, String> dependencies = new HashMap<>();

    // parent id → list of child ids
    private final Map<String, List<String>> dependents = new HashMap<>();

    // ── add_backup ────────────────────────────────────────────────────────────
    public void add_backup(String id, long endTime, long retentionMs) {
        if (backups.containsKey(id)) {
            throw new IllegalArgumentException("Backup already exists: " + id);
        }
        backups.put(id, new Backup(id, endTime, retentionMs));
        dependents.putIfAbsent(id, new ArrayList<>());
    }

    // ── add_dependency ────────────────────────────────────────────────────────
    // childId depends on parentId
    // e.g. add_dependency("B2", "B1") means B2 is an incremental on top of B1
    public void add_dependency(String childId, String parentId) {
        if (!backups.containsKey(childId)) throw new IllegalArgumentException("Unknown child: "  + childId);
        if (!backups.containsKey(parentId)) throw new IllegalArgumentException("Unknown parent: " + parentId);
        if (dependencies.containsKey(childId)) {
            throw new IllegalStateException(childId + " already has a parent");
        }
        // cycle detection — walk up from parentId, if we reach childId there's a cycle
        String cursor = parentId;
        while (cursor != null) {
            if (cursor.equals(childId)) {
                throw new IllegalStateException("Cycle detected adding " + childId + " -> " + parentId);
            }
            cursor = dependencies.get(cursor);
        }
        dependencies.put(childId, parentId);
        dependents.computeIfAbsent(parentId, k -> new ArrayList<>()).add(childId);
    }

    // ── find_recovery_chain ───────────────────────────────────────────────────
    // Returns the ordered list of backups needed to restore backupId
    // Walks up the dependency chain to the root (full backup)
    // Returns [root, ..., backupId] — oldest first
    public List<String> find_recovery_chain(String backupId) {
        if (!backups.containsKey(backupId)) {
            throw new IllegalArgumentException("Unknown backup: " + backupId);
        }

        LinkedList<String> chain = new LinkedList<>();
        String cursor = backupId;

        // Walk up to root
        while (cursor != null) {
            chain.addFirst(cursor); // prepend — so root ends up first
            cursor = dependencies.get(cursor);
        }

        return new ArrayList<>(chain);
    }

    // ── expire_backups ────────────────────────────────────────────────────────
    // Removes all backups whose expiresAt < now
    // Also cascades — if a parent is expired, all its dependents must also expire
    // (you can't restore a child without its parent)
    // Returns the list of expired backup ids
    public List<String> expire_backups(long now) {
        List<String> expired = new ArrayList<>();

        // Find all directly expired backups
        Set<String> toExpire = new HashSet<>();
        for (Backup b : backups.values()) {
            if (b.isExpired(now)) {
                toExpire.add(b.id);
            }
        }

        // Cascade — BFS/DFS from each expired node to mark dependents expired too
        // A child cannot survive without its parent
        Queue<String> queue = new LinkedList<>(toExpire);
        while (!queue.isEmpty()) {
            String id = queue.poll();
            if (toExpire.add(id)) { // newly added — process its children
                // already in set if added initially, but cascade adds new ones
            }
            // cascade to all children of this expired backup
            List<String> children = dependents.getOrDefault(id, Collections.emptyList());
            for (String child : children) {
                if (!toExpire.contains(child)) {
                    toExpire.add(child);
                    queue.offer(child);
                }
            }
        }

        // Remove all expired
        for (String id : toExpire) {
            backups.remove(id);
            dependencies.remove(id);
            dependents.remove(id);
            // remove this id from its parent's dependents list
            String parent = dependencies.remove(id);
            if (parent != null && dependents.containsKey(parent)) {
                dependents.get(parent).remove(id);
            }
            expired.add(id);
        }

        return expired;
    }

    // ── get_all_backups ────────────────────────────────────────────────────────
    public Collection<Backup> get_all_backups() {
        return Collections.unmodifiableCollection(backups.values());
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PART 3 — THREAD-SAFE VERSION
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy:
//   - Single ReadWriteLock across all operations
//   - Read  lock : find_recovery_chain (read-only traversal)
//   - Write lock : add_backup, add_dependency, expire_backups (mutate state)
//
// Why not ConcurrentHashMap alone?
//   Multi-step operations like add_dependency (check + insert + update reverse map)
//   must be atomic as a whole. ConcurrentHashMap only makes individual operations
//   atomic — we need the whole method to be atomic.
//
// Why ReadWriteLock over synchronized?
//   find_recovery_chain is read-only — multiple threads can call it concurrently.
//   synchronized would serialize ALL calls including concurrent reads.
//   ReadWriteLock allows N concurrent readers, exclusive writer.

class ThreadSafeBackupSystem {

    private final Map<String, Backup>        backups      = new HashMap<>();
    private final Map<String, String>        dependencies = new HashMap<>();
    private final Map<String, List<String>>  dependents   = new HashMap<>();

    // Single RW lock for the entire system
    private final ReadWriteLock rwLock    = new ReentrantReadWriteLock();
    private final Lock          readLock  = rwLock.readLock();
    private final Lock          writeLock = rwLock.writeLock();

    // ── add_backup ────────────────────────────────────────────────────────────
    public void add_backup(String id, long endTime, long retentionMs) {
        writeLock.lock();
        try {
            if (backups.containsKey(id)) throw new IllegalArgumentException("Duplicate: " + id);
            backups.put(id, new Backup(id, endTime, retentionMs));
            dependents.putIfAbsent(id, new ArrayList<>());
        } finally {
            writeLock.unlock(); // ALWAYS in finally
        }
    }

    // ── add_dependency ────────────────────────────────────────────────────────
    public void add_dependency(String childId, String parentId) {
        writeLock.lock();
        try {
            if (!backups.containsKey(childId))  throw new IllegalArgumentException("Unknown child: "  + childId);
            if (!backups.containsKey(parentId)) throw new IllegalArgumentException("Unknown parent: " + parentId);
            if (dependencies.containsKey(childId)) throw new IllegalStateException("Already has parent: " + childId);

            // Cycle detection — must be inside write lock so graph doesn't change under us
            String cursor = parentId;
            while (cursor != null) {
                if (cursor.equals(childId)) throw new IllegalStateException("Cycle detected");
                cursor = dependencies.get(cursor);
            }

            dependencies.put(childId, parentId);
            dependents.computeIfAbsent(parentId, k -> new ArrayList<>()).add(childId);
        } finally {
            writeLock.unlock();
        }
    }

    // ── find_recovery_chain ───────────────────────────────────────────────────
    // READ LOCK — multiple threads can call this concurrently
    public List<String> find_recovery_chain(String backupId) {
        readLock.lock();
        try {
            if (!backups.containsKey(backupId)) throw new IllegalArgumentException("Unknown: " + backupId);

            LinkedList<String> chain = new LinkedList<>();
            String cursor = backupId;
            while (cursor != null) {
                chain.addFirst(cursor);
                cursor = dependencies.get(cursor);
            }
            return new ArrayList<>(chain);
        } finally {
            readLock.unlock();
        }
    }

    // ── expire_backups ────────────────────────────────────────────────────────
    // WRITE LOCK — mutates all three maps
    public List<String> expire_backups(long now) {
        writeLock.lock();
        try {
            List<String> expired = new ArrayList<>();
            Set<String> toExpire = new HashSet<>();

            // Find directly expired
            for (Backup b : backups.values()) {
                if (b.isExpired(now)) toExpire.add(b.id);
            }

            // Cascade to dependents via BFS
            Queue<String> queue = new LinkedList<>(toExpire);
            while (!queue.isEmpty()) {
                String id = queue.poll();
                for (String child : dependents.getOrDefault(id, Collections.emptyList())) {
                    if (toExpire.add(child)) {
                        queue.offer(child);
                    }
                }
            }

            // Remove all expired from all maps
            for (String id : toExpire) {
                backups.remove(id);
                String parent = dependencies.remove(id);
                if (parent != null && dependents.containsKey(parent)) {
                    dependents.get(parent).remove(id);
                }
                dependents.remove(id);
                expired.add(id);
            }

            return expired;
        } finally {
            writeLock.unlock();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PART 4 — TESTS
// ─────────────────────────────────────────────────────────────────────────────

public class BackupSystem {

    static void testSingleThreaded() {
        System.out.println("\n═══ SINGLE-THREADED TESTS ═══\n");

        BackupSystem sys = new BackupSystem();
        long now = 1000L;

        // B1 = full backup, expires at t=2000
        sys.add_backup("B1", now,       1000);
        // B2 = incremental on B1, expires at t=3000
        sys.add_backup("B2", now + 500, 2000);
        // B3 = incremental on B2, expires at t=2500
        sys.add_backup("B3", now + 200, 1300);
        // B4 = another incremental on B1
        sys.add_backup("B4", now + 100, 5000);

        // B2 depends on B1, B3 depends on B2, B4 depends on B1
        sys.add_dependency("B2", "B1");
        sys.add_dependency("B3", "B2");
        sys.add_dependency("B4", "B1");

        // find_recovery_chain
        System.out.println("Recovery chain for B3: " + sys.find_recovery_chain("B3"));
        // Expected: [B1, B2, B3]
        System.out.println("Recovery chain for B4: " + sys.find_recovery_chain("B4"));
        // Expected: [B1, B4]
        System.out.println("Recovery chain for B1: " + sys.find_recovery_chain("B1"));
        // Expected: [B1]

        // expire_backups at t=2100
        // B1 expires at 1000+1000=2000 → expired
        // B1 expired → B2 and B4 cascade expire (children of B1)
        // B2 expired → B3 cascades expire
        long expireTime = 2100L;
        List<String> expired = sys.expire_backups(expireTime);
        System.out.println("\nExpired at t=" + expireTime + ": " + expired);
        // Expected: [B1, B2, B3, B4] in some order
        System.out.println("Remaining backups: " + sys.get_all_backups());
        // Expected: empty
    }

    static void testThreadSafe() throws InterruptedException {
        System.out.println("\n═══ THREAD-SAFE TESTS ═══\n");

        ThreadSafeBackupSystem sys = new ThreadSafeBackupSystem();
        long now = 1000L;

        // Setup backups
        sys.add_backup("B1", now,       10000); // long retention
        sys.add_backup("B2", now + 100, 10000);
        sys.add_backup("B3", now + 200, 10000);
        sys.add_backup("B4", now + 300, 10000);
        sys.add_backup("B5", now + 400, 10000);

        sys.add_dependency("B2", "B1");
        sys.add_dependency("B3", "B2");
        sys.add_dependency("B4", "B1");
        sys.add_dependency("B5", "B4");

        // Concurrent recovery chain reads — should all work in parallel
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<List<String>>> futures = new ArrayList<>();

        for (String id : List.of("B3", "B5", "B2", "B4")) {
            final String bid = id;
            futures.add(pool.submit(() -> sys.find_recovery_chain(bid)));
        }

        for (Future<List<String>> f : futures) {
            try {
                System.out.println("Chain: " + f.get());
            } catch (ExecutionException e) {
                System.out.println("Error: " + e.getCause().getMessage());
            }
        }

        // Concurrent add_dependency — only one should succeed, others throw
        System.out.println("\nTesting concurrent add_dependency (only first should succeed):");
        List<Future<?>> depFutures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            depFutures.add(pool.submit(() -> {
                try {
                    sys.add_dependency("B3", "B4"); // B3 already has parent B2 — should throw
                    System.out.println(Thread.currentThread().getName() + ": added (shouldn't happen)");
                } catch (Exception e) {
                    System.out.println(Thread.currentThread().getName() + ": correctly rejected — " + e.getMessage());
                }
            }));
        }
        for (Future<?> f : depFutures) {
            try { f.get(); } catch (ExecutionException e) { /* handled above */ }
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws InterruptedException {
        testSingleThreaded();
        testThreadSafe();
    }
}
