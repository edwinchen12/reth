//! Transaction wrapper for libmdbx-sys.

use super::cursor::Cursor;
use crate::{
    metrics::{DatabaseEnvMetrics, Operation, TransactionMode, TransactionOutcome},
    tables::utils::decode_one,
    DatabaseError,
};
use reth_db_api::{
    table::{Compress, DupSort, Encode, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
};
use reth_libmdbx::{ffi::MDBX_dbi, CommitLatency, Transaction, TransactionKind, WriteFlags, RW};
use reth_storage_errors::db::{DatabaseErrorInfo, DatabaseWriteError, DatabaseWriteOperation};
use reth_tracing::tracing::{debug, trace, warn};
use std::{backtrace::Backtrace, cmp, fmt, marker::PhantomData, sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
}, time::{Duration, Instant}};
use std::borrow::Cow;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, HashMap};
use std::sync::RwLock;
use redis::{Client, Commands, ErrorKind, Iter, Pipeline, RedisError, RedisResult, Value as RedisValue};

/// Duration after which we emit the log about long-lived database transactions.
const LONG_TRANSACTION_DURATION: Duration = Duration::from_secs(60);

struct CachedValue {
    sorted_set_key: String,
    dup_set_key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

unsafe fn memcmp(s1: &[u8], s2: &[u8], n: usize) -> i32 {
    for i in 0..n {
        let a = *s1.get_unchecked(i);
        let b = *s2.get_unchecked(i);
        if a != b {
            return (a as i32).saturating_sub(b as i32);
        };
    }

    0
}
impl Eq for CachedValue {}

impl PartialEq<Self> for CachedValue {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl PartialOrd<Self> for CachedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CachedValue {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        let min_length = cmp::min(self.value.clone().unwrap().len(), other.value.clone().unwrap().len());
        let res = unsafe { memcmp(&self.value.clone().unwrap()[..], &other.value.clone().unwrap()[..], min_length) };
        if res > 0 {
            CmpOrdering::Greater
        } else if res < 0 {
            CmpOrdering::Less
        } else {
            if self.value.clone().unwrap().len() > other.value.clone().unwrap().len() {
                CmpOrdering::Greater
            } else if self.value.clone().unwrap().len() < other.value.clone().unwrap().len() {
                CmpOrdering::Less
            } else {
                CmpOrdering::Equal
            }
        }
    }
}

/// Wrapper for the libmdbx transaction.
pub struct Tx<K: TransactionKind> {
    /// Libmdbx-sys transaction.
    pub inner: Transaction<K>,

    /// redis client
    redis: Arc<Client>,

    /// Handler for metrics with its own [Drop] implementation for cases when the transaction isn't
    /// closed by [`Tx::commit`] or [`Tx::abort`], but we still need to report it in the metrics.
    ///
    /// If [Some], then metrics are reported.
    metrics_handler: Option<MetricsHandler<K>>,

    /// redis pipeline holding the changes in the transaction.
    pipeline: Arc<RwLock<Pipeline>>,

    /// redis data that has been changed in the transaction but not yet committed.
    uncommitted_data: Arc<RwLock<HashMap<Vec<u8>, CachedValue>>>,

    /// redis key that has been deleted in the transaction but not yet committed.
    deleted_keys: Arc<RwLock<HashMap<Vec<u8>, CachedValue>>>,

    // redis data with duplication that has been changed in the transaction but not yet commited.
    uncommited_data_dup: Arc<RwLock<HashMap<Vec<u8>, BTreeSet<CachedValue>>>>,

    // redis data with duplication that has been deleted in the transaction.
    deleted_data_dup: Arc<RwLock<HashMap<Vec<u8>, BTreeSet<CachedValue>>>>
}

impl<K: TransactionKind> Tx<K> {
    /// Creates new `Tx` object with a `RO` or `RW` transaction.
    #[inline]
    pub const fn new(
        inner: Transaction<K>,
        redis: Arc<Client>,
        pipeline: Arc<RwLock<Pipeline>>,
        uncommitted_data: Arc<RwLock<HashMap<Vec<u8>, CachedValue>>>,
        deleted_keys: Arc<RwLock<HashMap<Vec<u8>, CachedValue>>>,
        uncommited_data_dup: Arc<RwLock<HashMap<Vec<u8>, BTreeSet<CachedValue>>>>,
        deleted_data_dup: Arc<RwLock<HashMap<Vec<u8>, BTreeSet<CachedValue>>>>
    ) -> Self {
        Self::new_inner(inner, redis, None, pipeline, uncommitted_data, deleted_keys, uncommited_data_dup, deleted_data_dup)
    }

    /// Creates new `Tx` object with a `RO` or `RW` transaction and optionally enables metrics.
    #[inline]
    #[track_caller]
    pub(crate) fn new_with_metrics(
        inner: Transaction<K>,
        redis: Arc<Client>,
        env_metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> reth_libmdbx::Result<Self> {
        let metrics_handler = env_metrics
            .map(|env_metrics| {
                let handler = MetricsHandler::<K>::new(inner.id()?, env_metrics);
                handler.env_metrics.record_opened_transaction(handler.transaction_mode());
                handler.log_transaction_opened();
                Ok(handler)
            })
            .transpose()?;
        Ok(Self::new_inner(inner, redis, metrics_handler, Arc::new(RwLock::new(Pipeline::new())), Arc::new(RwLock::new(HashMap::new())), Arc::new(RwLock::new(HashMap::new())), Arc::new(RwLock::new(HashMap::new())), Arc::new(RwLock::new(HashMap::new()))))
    }

    #[inline]
    const fn new_inner(
        inner: Transaction<K>,
        redis: Arc<Client>,
        metrics_handler: Option<MetricsHandler<K>>,
        pipeline: Arc<RwLock<Pipeline>>,
        uncommitted_data: Arc<RwLock<HashMap<Vec<u8>, CachedValue>>>,
        deleted_keys: Arc<RwLock<HashMap<Vec<u8>, CachedValue>>>,
        uncommited_data_dup: Arc<RwLock<HashMap<Vec<u8>, BTreeSet<CachedValue>>>>,
        deleted_data_dup: Arc<RwLock<HashMap<Vec<u8>, BTreeSet<CachedValue>>>>
    ) -> Self {
        Self { inner, redis, metrics_handler, pipeline, uncommitted_data, deleted_keys, uncommited_data_dup, deleted_data_dup }
    }

    /// Gets this transaction ID.
    pub fn id(&self) -> reth_libmdbx::Result<u64> {
        self.metrics_handler.as_ref().map_or_else(|| self.inner.id(), |handler| Ok(handler.txn_id))
    }

    /// Gets a table database handle if it exists, otherwise creates it.
    pub fn get_dbi<T: Table>(&self) -> Result<MDBX_dbi, DatabaseError> {
        self.inner
            .open_db(Some(T::NAME))
            .map(|db| db.dbi())
            .map_err(|e| DatabaseError::Open(e.into()))
    }

    /// Create db Cursor
    pub fn new_cursor<T: Table>(&self) -> Result<Cursor<K, T>, DatabaseError> {
        let inner = self
            .inner
            .cursor_with_dbi(self.get_dbi::<T>()?)
            .map_err(|e| DatabaseError::InitCursor(e.into()))?;

        Ok(Cursor::new_with_metrics(
            inner,
            Arc::clone(&self.redis),
            self.metrics_handler.as_ref().map(|h| h.env_metrics.clone()),
        ))
    }

    /// If `self.metrics_handler == Some(_)`, measure the time it takes to execute the closure and
    /// record a metric with the provided transaction outcome.
    ///
    /// Otherwise, just execute the closure.
    fn execute_with_close_transaction_metric<R>(
        mut self,
        outcome: TransactionOutcome,
        f: impl FnOnce(Self) -> (R, Option<CommitLatency>),
    ) -> R {
        let run = |tx| {
            let start = Instant::now();
            let (result, commit_latency) = f(tx);
            let total_duration = start.elapsed();

            if outcome.is_commit() {
                debug!(
                    target: "storage::db::mdbx",
                    ?total_duration,
                    ?commit_latency,
                    is_read_only = K::IS_READ_ONLY,
                    "Commit"
                );
            }

            (result, commit_latency, total_duration)
        };

        if let Some(mut metrics_handler) = self.metrics_handler.take() {
            metrics_handler.close_recorded = true;
            metrics_handler.log_backtrace_on_long_read_transaction();

            let (result, commit_latency, close_duration) = run(self);
            let open_duration = metrics_handler.start.elapsed();
            metrics_handler.env_metrics.record_closed_transaction(
                metrics_handler.transaction_mode(),
                outcome,
                open_duration,
                Some(close_duration),
                commit_latency,
            );

            result
        } else {
            run(self).0
        }
    }

    /// If `self.metrics_handler == Some(_)`, measure the time it takes to execute the closure and
    /// record a metric with the provided operation.
    ///
    /// Otherwise, just execute the closure.
    fn execute_with_operation_metric<T: Table, R>(
        &self,
        operation: Operation,
        value_size: Option<usize>,
        f: impl FnOnce(&Transaction<K>) -> R,
    ) -> R {
        if let Some(metrics_handler) = &self.metrics_handler {
            metrics_handler.log_backtrace_on_long_read_transaction();
            metrics_handler
                .env_metrics
                .record_operation(T::NAME, operation, value_size, || f(&self.inner))
        } else {
            f(&self.inner)
        }
    }

    /// Generates a Redis key for the given table and key.
    /// TODO: optimize this function
    fn generate_redis_key<T: Table>(key: &T::Key) -> Vec<u8> {
        let prefix = format!("{}:", T::NAME);
        let encoded_key = key.clone().encode();
        let encoded_key= encoded_key.as_ref();
        [prefix.encode().as_slice(), encoded_key].concat()
    }

    fn from_redis_key(key: Vec<u8>) -> Vec<u8> {
        let table_key: String = String::from_utf8(key).unwrap().split(":").take(1).collect();
        table_key.as_bytes().to_vec()
    }

    fn generate_sorted_set_key<T: Table>() -> String {
        format!("sorted:{}",T::NAME)
    }

    fn generate_dup_set_key<T: Table>(key: &T::Key) -> Vec<u8> {
        let prefix = format!("{}:", T::NAME);
        let encoded_key = key.clone().encode();
        let encoded_key= encoded_key.as_ref();
        [prefix.encode().as_slice(), encoded_key].concat()
    }

    fn clear_redis<T: Table>(&self) -> Result<(), DatabaseError> {
        let mut connection = self.redis.get_connection()
            .map_err(|e| DatabaseError::Open(from(e)))?;

        let pattern = format!("{}:*", T::NAME);
        let iter: Iter<'_, String> = connection.scan_match(&pattern)
            .map_err(|e| DatabaseError::Read(from(e)))?;

        let keys: Vec<String> = iter.collect();

        for key in keys {
            let _: () = connection.del(key).map_err(|e| DatabaseError::Delete(from(e)))?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct MetricsHandler<K: TransactionKind> {
    /// Cached internal transaction ID provided by libmdbx.
    txn_id: u64,
    /// The time when transaction has started.
    start: Instant,
    /// Duration after which we emit the log about long-lived database transactions.
    long_transaction_duration: Duration,
    /// If `true`, the metric about transaction closing has already been recorded and we don't need
    /// to do anything on [`Drop::drop`].
    close_recorded: bool,
    /// If `true`, the backtrace of transaction will be recorded and logged.
    /// See [`MetricsHandler::log_backtrace_on_long_read_transaction`].
    record_backtrace: bool,
    /// If `true`, the backtrace of transaction has already been recorded and logged.
    /// See [`MetricsHandler::log_backtrace_on_long_read_transaction`].
    backtrace_recorded: AtomicBool,
    /// Shared database environment metrics.
    env_metrics: Arc<DatabaseEnvMetrics>,
    /// Backtrace of the location where the transaction has been opened. Reported only with debug
    /// assertions, because capturing the backtrace on every transaction opening is expensive.
    #[cfg(debug_assertions)]
    open_backtrace: Backtrace,
    _marker: PhantomData<K>,
}

impl<K: TransactionKind> MetricsHandler<K> {
    fn new(txn_id: u64, env_metrics: Arc<DatabaseEnvMetrics>) -> Self {
        Self {
            txn_id,
            start: Instant::now(),
            long_transaction_duration: LONG_TRANSACTION_DURATION,
            close_recorded: false,
            record_backtrace: true,
            backtrace_recorded: AtomicBool::new(false),
            #[cfg(debug_assertions)]
            open_backtrace: Backtrace::force_capture(),
            env_metrics,
            _marker: PhantomData,
        }
    }

    const fn transaction_mode(&self) -> TransactionMode {
        if K::IS_READ_ONLY {
            TransactionMode::ReadOnly
        } else {
            TransactionMode::ReadWrite
        }
    }

    /// Logs the caller location and ID of the transaction that was opened.
    #[track_caller]
    fn log_transaction_opened(&self) {
        trace!(
            target: "storage::db::mdbx",
            caller = %core::panic::Location::caller(),
            id = %self.txn_id,
            mode = %self.transaction_mode().as_str(),
            "Transaction opened",
        );
    }

    /// Logs the backtrace of current call if the duration that the read transaction has been open
    /// is more than [`LONG_TRANSACTION_DURATION`] and `record_backtrace == true`.
    /// The backtrace is recorded and logged just once, guaranteed by `backtrace_recorded` atomic.
    ///
    /// NOTE: Backtrace is recorded using [`Backtrace::force_capture`], so `RUST_BACKTRACE` env var
    /// is not needed.
    fn log_backtrace_on_long_read_transaction(&self) {
        if self.record_backtrace &&
            !self.backtrace_recorded.load(Ordering::Relaxed) &&
            self.transaction_mode().is_read_only()
        {
            let open_duration = self.start.elapsed();
            if open_duration >= self.long_transaction_duration {
                self.backtrace_recorded.store(true, Ordering::Relaxed);
                #[cfg(debug_assertions)]
                let message = format!(
                   "The database read transaction has been open for too long. Open backtrace:\n{}\n\nCurrent backtrace:\n{}",
                   self.open_backtrace,
                   Backtrace::force_capture()
                );
                #[cfg(not(debug_assertions))]
                let message = format!(
                    "The database read transaction has been open for too long. Backtrace:\n{}",
                    Backtrace::force_capture()
                );
                warn!(
                    target: "storage::db::mdbx",
                    ?open_duration,
                    %self.txn_id,
                    "{message}"
                );
            }
        }
    }
}

impl<K: TransactionKind> Drop for MetricsHandler<K> {
    fn drop(&mut self) {
        if !self.close_recorded {
            self.log_backtrace_on_long_read_transaction();
            self.env_metrics.record_closed_transaction(
                self.transaction_mode(),
                TransactionOutcome::Drop,
                self.start.elapsed(),
                None,
                None,
            );
        }
    }
}

impl<K: TransactionKind> fmt::Debug for Tx<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx")
            .field("inner", &self.inner)
            .field("redis", &self.redis)
            .field("metrics_handler", &self.metrics_handler)
            // .field("uncommitted_data", &self.uncommitted_data)
            // .field("deleted_keys", &self.deleted_keys)
            .finish()
    }
}

impl TableImporter for Tx<RW> {}

// TODO We might need to use the mdbx implementation for read first given that we will use historical data provider to support client requests
impl<K: TransactionKind> DbTx for Tx<K> {
    type Cursor<T: Table> = Cursor<K, T>;
    type DupCursor<T: DupSort> = Cursor<K, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<<T as Table>::Value>, DatabaseError> {
        // let key: <<T as Table>::Key as Encode>::Encoded = key.encode();
        let redis_key = Tx::<K>::generate_redis_key::<T>(&key);
        self.execute_with_operation_metric::<T, _>(Operation::Get, None, |tx| {
            if self.deleted_keys.read().unwrap().contains_key(&redis_key) {
                Ok(None)
            } else if let Some(value) = self.uncommitted_data.read().unwrap().get(&redis_key) {
                Ok(Some(decode_one::<T>(Cow::Owned(value.value.clone().unwrap().to_vec()))?))
            } else {
                // TODO: review the isolation for this case, should we read from redis?
                let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e)))?;
                let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;
                if let Some(value) = value {
                    Ok(Some(decode_one::<T>(Cow::Owned(value))?))
                } else {
                    Ok(None)
                }
            }
        })
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        self.execute_with_operation_metric::<T, _>(Operation::Get, None, |tx| {
            tx.get(self.get_dbi::<T>()?, key.as_ref())
                .map_err(|e| DatabaseError::Read(e.into()))?
                .map(decode_one::<T>)
                .transpose()
        })
    }

    //TODO: do we need to empty the local cache?
    //TODO: we might need to add the key to sorted set as well
    fn commit(self) -> Result<bool, DatabaseError> {
        self.execute_with_close_transaction_metric(TransactionOutcome::Commit, |this| {
            let mut connection = this.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
            this.pipeline.write().unwrap().execute(&mut connection);

            for (key, val) in this.deleted_keys.read().unwrap().iter() {
                let sorted_set_key = val.sorted_set_key.clone();
                let _: RedisResult<RedisValue> = connection.zrem(sorted_set_key, Self::from_redis_key(key.clone()));
            }

            for (key, set) in this.uncommited_data_dup.read().unwrap().iter() {
                let mut sorted_set_key: String = String::new();
                for (val) in set.iter() {
                    let dup_set_key = val.dup_set_key.clone();
                    let _: RedisResult<RedisValue> = connection.zrem(dup_set_key, val.value.clone().unwrap());

                    sorted_set_key = val.sorted_set_key.clone();
                }

                if !sorted_set_key.is_empty() {
                    let _: RedisResult<RedisValue> = connection.zrem(sorted_set_key, Self::from_redis_key(key.clone()));
                }
            }

            this.deleted_keys.write().unwrap().clear();
            this.uncommitted_data.write().unwrap().clear();
            this.deleted_data_dup.write().unwrap().clear();
            this.uncommited_data_dup.write().unwrap().clear();

            match this.inner.commit().map_err(|e| DatabaseError::Commit(e.into())) {
                Ok((v, latency)) => (Ok(v), Some(latency)),
                Err(e) => (Err(e), None),
            }
        })
    }

    fn abort(self) {
        self.execute_with_close_transaction_metric(TransactionOutcome::Abort, |this| {
            (drop(this.inner), None)
        })
    }

    // Iterate over read only values in database.
    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.new_cursor()
    }

    /// Iterate over read only values in database.
    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.new_cursor()
    }

    /// Returns number of entries in the table using cheap DB stats invocation.
    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        Ok(self
            .inner
            .db_stat_with_dbi(self.get_dbi::<T>()?)
            .map_err(|e| DatabaseError::Stats(e.into()))?
            .entries())
    }

    /// Disables long-lived read transaction safety guarantees, such as backtrace recording and
    /// timeout.
    fn disable_long_read_transaction_safety(&mut self) {
        if let Some(metrics_handler) = self.metrics_handler.as_mut() {
            metrics_handler.record_backtrace = false;
        }

        self.inner.disable_timeout();
    }
}

impl DbTxMut for Tx<RW> {
    type CursorMut<T: Table> = Cursor<RW, T>;
    type DupCursorMut<T: DupSort> = Cursor<RW, T>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let set_key = key.clone();
        let redis_key = Tx::<RW>::generate_redis_key::<T>(&key);

        let key = key.encode();
        let compressed_value = value.compress();

        if T::is_dup_sort() {
            let dup_set_key = Self::generate_dup_set_key::<T>(&set_key);
            self.uncommited_data_dup.write().unwrap().entry(redis_key.clone()).or_insert(BTreeSet::new()).insert(CachedValue{
                value: Some(compressed_value.as_ref().to_vec()),
                sorted_set_key: Self::generate_sorted_set_key::<T>(),
                dup_set_key: Some(dup_set_key.clone())
            });
            self.pipeline.write().unwrap().zadd(dup_set_key, compressed_value.as_ref().to_vec(), 0);
        } else {
            self.uncommitted_data.write().unwrap().insert(redis_key.clone(), CachedValue{
                value: Some(compressed_value.as_ref().to_vec()),
                sorted_set_key: Self::generate_sorted_set_key::<T>(),
                dup_set_key: None
            });
            self.pipeline.write().unwrap().set(redis_key.clone(), compressed_value.as_ref());
        }

        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let sorted_set_key = Tx::<RW>::generate_sorted_set_key::<T>();
        let _: RedisResult<RedisValue> = connection.zadd(sorted_set_key, set_key.encode().as_ref(), 0);

        self.execute_with_operation_metric::<T, _>(
            Operation::Put,
            Some(compressed_value.as_ref().len()),
            |tx| {
                tx.put(self.get_dbi::<T>()?, key.as_ref(), compressed_value, WriteFlags::UPSERT).map_err(|e| {
                    DatabaseWriteError {
                        info: e.into(),
                        operation: DatabaseWriteOperation::Put,
                        table_name: T::NAME,
                        key: key.into(),
                    }
                    .into()
                })
            },
        )
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let mut data = None;
        let set_key = key.clone();
        let value = value.map(Compress::compress);
        if let Some(value) = &value {
            data = Some(value.as_ref());
        };

        let redis_key = Tx::<RW>::generate_redis_key::<T>(&key);
        let key = key.encode();

        if T::is_dup_sort() {
            let mut dup_set_write = self.deleted_data_dup.write().unwrap();
            let dup_set_option = dup_set_write.get_mut(&redis_key.clone());
            let  dup_set = dup_set_option.unwrap();
            let cached_value = CachedValue{
                value: Some(data.unwrap().clone().to_vec()),
                sorted_set_key: Self::generate_sorted_set_key::<T>(),
                dup_set_key: Some(Self::generate_dup_set_key::<T>(&set_key))
            };

            let _ = dup_set.remove(&cached_value);
            self.pipeline.write().unwrap().zrem(cached_value.dup_set_key, cached_value.value);
        } else {
            self.deleted_keys.write().unwrap().insert(redis_key.clone(), CachedValue{value: None, sorted_set_key: Self::generate_sorted_set_key::<T>(), dup_set_key: None});
            self.pipeline.write().unwrap().del(redis_key.clone());
        }

        self.execute_with_operation_metric::<T, _>(Operation::Delete, None, |tx| {
            let key = key;
            tx.del(self.get_dbi::<T>()?, key.as_ref(), data)
                .map_err(|e| DatabaseError::Delete(e.into()))
        })
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        self.clear_redis::<T>()?;
        self.inner.clear_db(self.get_dbi::<T>()?).map_err(|e| DatabaseError::Delete(e.into()))?;

        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        self.new_cursor()
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        self.new_cursor()
    }
}

#[cfg(test)]
mod tests {
    use crate::implementation::redis::{DatabaseArguments, tables, DatabaseEnv, DatabaseEnvKind};
    use reth_db_api::{database::Database, models::ClientVersion, transaction::DbTx};
    use reth_libmdbx::MaxReadTransactionDuration;
    use reth_storage_errors::db::DatabaseError;
    use std::{sync::atomic::Ordering, thread::sleep, time::Duration};
    use tempfile::tempdir;

    // TODO re-enable the tests
    // #[test]
    // fn long_read_transaction_safety_disabled() {
    //     const MAX_DURATION: Duration = Duration::from_secs(1);
    //
    //     let dir = tempdir().unwrap();
    //     let args = DatabaseArguments::new(ClientVersion::default())
    //         .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Set(
    //             MAX_DURATION,
    //         )));
    //     let db = DatabaseEnv::open("redis://localhost:6379".as_ref(), dir.path(), DatabaseEnvKind::RW, args).unwrap().with_metrics();
    //
    //     let mut tx = db.tx().unwrap();
    //     tx.metrics_handler.as_mut().unwrap().long_transaction_duration = MAX_DURATION;
    //     tx.disable_long_read_transaction_safety();
    //     // Give the `TxnManager` some time to time out the transaction.
    //     sleep(MAX_DURATION + Duration::from_millis(100));
    //
    //     // Transaction has not timed out.
    //     assert_eq!(
    //         tx.get::<tables::Transactions>(0),
    //         Err(DatabaseError::Open(reth_libmdbx::Error::NotFound.into()))
    //     );
    //     // Backtrace is not recorded.
    //     assert!(!tx.metrics_handler.unwrap().backtrace_recorded.load(Ordering::Relaxed));
    // }

    // #[test]
    // fn long_read_transaction_safety_enabled() {
    //     const MAX_DURATION: Duration = Duration::from_secs(1);
    //
    //     let dir = tempdir().unwrap();
    //     let args = DatabaseArguments::new(ClientVersion::default())
    //         .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Set(
    //             MAX_DURATION,
    //         )));
    //     let db = DatabaseEnv::open("redis://localhost:6379".as_ref(), dir.path(), DatabaseEnvKind::RW, args).unwrap().with_metrics();
    //
    //     let mut tx = db.tx().unwrap();
    //     tx.metrics_handler.as_mut().unwrap().long_transaction_duration = MAX_DURATION;
    //     // Give the `TxnManager` some time to time out the transaction.
    //     sleep(MAX_DURATION + Duration::from_millis(100));
    //
    //     // Transaction has timed out.
    //     assert_eq!(
    //         tx.get::<tables::Transactions>(0),
    //         Err(DatabaseError::Open(reth_libmdbx::Error::ReadTransactionTimeout.into()))
    //     );
    //     // Backtrace is recorded.
    //     assert!(tx.metrics_handler.unwrap().backtrace_recorded.load(Ordering::Relaxed));
    // }
}

#[inline]
fn from(value: RedisError) -> DatabaseErrorInfo {
    let code = match value.kind() {
        ErrorKind::ResponseError => 1,
        ErrorKind::ParseError => 2,
        ErrorKind::AuthenticationFailed => 3,
        ErrorKind::TypeError => 4,
        ErrorKind::ExecAbortError => 5,
        ErrorKind::BusyLoadingError => 6,
        ErrorKind::NoScriptError => 7,
        ErrorKind::InvalidClientConfig => 8,
        ErrorKind::Moved => 9,
        ErrorKind::Ask => 10,
        ErrorKind::TryAgain => 11,
        ErrorKind::ClusterDown => 12,
        ErrorKind::CrossSlot => 13,
        ErrorKind::MasterDown => 14,
        ErrorKind::IoError => 15,
        ErrorKind::ClientError => 16,
        ErrorKind::ExtensionError => 17,
        ErrorKind::ReadOnly => 18,
        ErrorKind::MasterNameNotFoundBySentinel => 19,
        ErrorKind::NoValidReplicasFoundBySentinel => 20,
        ErrorKind::EmptySentinelList => 21,
        ErrorKind::NotBusy => 22,
        ErrorKind::ClusterConnectionNotFound => 23,
        _ => 0,
    };
    DatabaseErrorInfo { message: Box::from(value.to_string()), code }
}
