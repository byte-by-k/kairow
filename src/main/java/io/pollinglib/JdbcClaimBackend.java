package io.pollinglib;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import io.pollinglib.PollingQueue.Claim;

/** The caller commits after the locking select and both batched updates. */
abstract class JdbcClaimBackend implements QueueBackend {
    final QueueMapping mapping;
    private final boolean mysql;
    private final String now;
    private final String deadline;

    JdbcClaimBackend(QueueMapping mapping, boolean mysql) {
        this.mapping = mapping;
        this.mysql = mysql;
        now = mysql ? "CURRENT_TIMESTAMP(6)" : "statement_timestamp()";
        deadline = mysql ? "TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6))"
                : "statement_timestamp() + ? * interval '1 millisecond'";
    }

    private Object id(UUID value) { return mysql ? value.toString() : value; }
    private long delay(Duration lease) {
        return mysql ? Math.multiplyExact(lease.toMillis(), 1000L) : lease.toMillis();
    }

    public Optional<Claim> claimDue(Connection tx, String owner, Duration lease, int shard, int count) throws SQLException {
        return claimBatch(tx, owner, lease, shard, count, 1).stream().findFirst();
    }

    public List<Claim> claimBatch(Connection tx, String owner, Duration lease, int shard, int count, int size) throws SQLException {
        String filter = "";
        if (count > 1) {
            String hash = mysql ? "CAST(CONV(LEFT(MD5(job.{id}),8),16,10) AS UNSIGNED)"
                    : "(('x' || left(md5(job.{id}::text),8))::bit(32)::bigint)";
            filter = " AND mod(" + hash + "," + count + ")=" + shard;
        }
        String sql = "SELECT job.{id}, job.{payload}, job.{attempts}, job.{max_attempts} FROM {table} job"
                + " WHERE job.{status} IN ('READY','PROCESSING') AND job.{available_at}<=" + now
                + filter + mapping.orderGuard() + " ORDER BY job.{available_at},job.{id} LIMIT ? FOR UPDATE SKIP LOCKED";
        try (var select = mapping.prepare(tx, sql);
             var retire = mapping.prepare(tx, "UPDATE {table} SET {status}='FAILED',{owner_id}=NULL,{claim_token}=NULL WHERE {id}=?");
             var take = mapping.prepare(tx, "UPDATE {table} SET {status}='PROCESSING',{attempts}={attempts}+1,"
                     + "{owner_id}=?,{claim_token}=?,{available_at}=" + deadline + " WHERE {id}=?")) {
            select.setInt(1, size);
            while (true) {
                var claims = new ArrayList<Claim>();
                int expired = 0;
                try (var rows = select.executeQuery()) {
                    while (rows.next()) {
                        UUID key = UUID.fromString(rows.getString(1));
                        if (rows.getInt(3) >= rows.getInt(4)) {
                            retire.setObject(1, id(key));
                            retire.addBatch();
                            expired++;
                        } else {
                            var claim = new Claim(key, rows.getString(2), rows.getInt(3)+1, UUID.randomUUID());
                            take.setString(1, owner);
                            take.setObject(2, id(claim.token()));
                            take.setLong(3, delay(lease));
                            take.setObject(4, id(key));
                            take.addBatch();
                            claims.add(claim);
                        }
                    }
                }
                if (expired > 0) retire.executeBatch();
                if (!claims.isEmpty()) take.executeBatch();
                if (!claims.isEmpty() || expired == 0) return claims;
            }
        }
    }

    public boolean fail(Connection tx, Claim claim) throws SQLException {
        return updateLease(tx, claim, null);
    }

    public boolean renew(Connection tx, Claim claim, Duration lease) throws SQLException {
        return updateLease(tx, claim, lease);
    }

    private boolean updateLease(Connection tx, Claim claim, Duration lease) throws SQLException {
        String change = lease == null ? "{status}='FAILED',{owner_id}=NULL,{claim_token}=NULL" : "{available_at}="+deadline;
        try (var s = mapping.prepare(tx, "UPDATE {table} SET "+change+" WHERE {id}=? AND {claim_token}=?"
                +" AND {status}='PROCESSING' AND {available_at}>"+now)) {
            int i=1;
            if (lease != null) s.setLong(i++, delay(lease));
            s.setObject(i++, id(claim.id()));
            s.setObject(i, id(claim.token()));
            return s.executeUpdate()==1;
        }
    }
}
