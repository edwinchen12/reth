//! Cursor wrapper for libmdbx-sys read/write to Redis

use crate::{
    metrics::{DatabaseEnvMetrics, Operation},
    tables::utils::*,
    DatabaseError,
};
use reth_db_api::{
    common::{PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{Compress, Decode, Decompress, DupSort, Encode, Table},
};
use reth_libmdbx::{Error as MDBXError, TransactionKind, WriteFlags, RO, RW};
use reth_storage_errors::db::{DatabaseErrorInfo, DatabaseWriteError, DatabaseWriteOperation};
use std::{borrow::Cow, collections::Bound, marker::PhantomData, ops::RangeBounds, sync::Arc};
use redis::{Client, Commands, ErrorKind, RedisError, RedisResult, ToRedisArgs, Value};

/// Read only Cursor.
pub type CursorRO<T> = Cursor<RO, T>;
/// Read write cursor.
pub type CursorRW<T> = Cursor<RW, T>;

/// Cursor wrapper to access KV items.
#[derive(Debug)]
pub struct Cursor<K: TransactionKind, T: Table> {
    /// Inner `libmdbx` cursor.
    pub(crate) inner: reth_libmdbx::Cursor<K>,
    /// Cache buffer that receives compressed values.
    buf: Vec<u8>,
    /// Reference to metric handles in the DB environment. If `None`, metrics are not recorded.
    metrics: Option<Arc<DatabaseEnvMetrics>>,
    /// Phantom data to enforce encoding/decoding.
    _dbi: PhantomData<T>,

    /// redis client
    redis: Arc<Client>,

    /// The current key the cursor is pointing to
    current_key: Option<T::Key>
}

pub struct StateROCursor<K: TransactionKind, T: Table> {
    /// Inner `libmdbx` cursor.
    pub(crate) inner: reth_libmdbx::Cursor<K>,
    /// Reference to metric handles in the DB environment. If `None`, metrics are not recorded.
    metrics: Option<Arc<DatabaseEnvMetrics>>,
    /// Phantom data to enforce encoding/decoding.
    _dbi: PhantomData<T>,

    /// redis client
    redis: Arc<Client>,

    /// The current key the cursor is pointing to
    current_key: Option<T::Key>
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
    DatabaseErrorInfo { message: value.to_string(), code }
}

impl<K: TransactionKind, T: Table> Cursor<K, T> {
    pub(crate) fn new_with_metrics(
        inner: reth_libmdbx::Cursor<K>,
        redis: Arc<Client>,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Self {
        // TODO cursor might need to be initialized with it pointing to the last element in the db.
        Self { inner, buf: Vec::new(), metrics, _dbi: PhantomData, redis, current_key: None }
    }

    /// If `self.metrics` is `Some(...)`, record a metric with the provided operation and value
    /// size.
    ///
    /// Otherwise, just execute the closure.
    fn execute_with_operation_metric<R>(
        &mut self,
        operation: Operation,
        value_size: Option<usize>,
        f: impl FnOnce(&mut Self) -> R,
    ) -> R {
        if let Some(metrics) = self.metrics.as_ref().cloned() {
            metrics.record_operation(T::NAME, operation, value_size, || f(self))
        } else {
            f(self)
        }
    }
}

impl<K: TransactionKind, T: Table> StateROCursor<K, T> {
    pub(crate) fn new_with_metrics(
        inner: reth_libmdbx::Cursor<K>,
        redis: Arc<Client>,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Self {
        Self { inner, metrics, _dbi: PhantomData, redis, current_key: None }
    }

    /// If `self.metrics` is `Some(...)`, record a metric with the provided operation and value
    /// size.
    ///
    /// Otherwise, just execute the closure.
    fn execute_with_operation_metric<R>(
        &mut self,
        operation: Operation,
        value_size: Option<usize>,
        f: impl FnOnce(&mut Self) -> R,
    ) -> R {
        if let Some(metrics) = self.metrics.as_ref().cloned() {
            metrics.record_operation(T::NAME, operation, value_size, || f(self))
        } else {
            f(self)
        }
    }
}

/// Decodes a `(key, value)` pair from the database.
#[allow(clippy::type_complexity)]
pub fn decode<T>(
    res: Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, impl Into<DatabaseErrorInfo>>,
) -> PairResult<T>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    res.map_err(|e| DatabaseError::Read(e.into()))?.map(decoder::<T>).transpose()
}

/// Some types don't support compression (eg. B256), and we don't want to be copying them to the
/// allocated buffer when we can just use their reference.
macro_rules! compress_to_buf_or_ref {
    ($self:expr, $value:expr) => {
        if let Some(value) = $value.uncompressable_ref() {
            Some(value)
        } else {
            $self.buf.truncate(0);
            $value.compress_to_buf(&mut $self.buf);
            None
        }
    };
}

// TODO: read the value from redis
impl<K: TransactionKind, T: Table> DbCursorRO<T> for Cursor<K, T> {
    fn first(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.first())
    }

    fn seek_exact(&mut self, key: <T as Table>::Key) -> PairResult<T> {
        decode::<T>(self.inner.set_key(key.encode().as_ref()))
    }

    fn seek(&mut self, key: <T as Table>::Key) -> PairResult<T> {
        decode::<T>(self.inner.set_range(key.encode().as_ref()))
    }

    fn next(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.next())
    }

    fn prev(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.prev())
    }

    fn last(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.last())
    }

    fn current(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.get_current())
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError> {
        let start = if let Some(start_key) = start_key {
            decode::<T>(self.inner.set_range(start_key.encode().as_ref())).transpose()
        } else {
            self.first().transpose()
        };

        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError> {
        let start = match range.start_bound().cloned() {
            Bound::Included(key) => self.inner.set_range(key.encode().as_ref()),
            Bound::Excluded(_key) => {
                unreachable!("Rust doesn't allow for Bound::Excluded in starting bounds");
            }
            Bound::Unbounded => self.inner.first(),
        };
        let start = decode::<T>(start).transpose();
        Ok(RangeWalker::new(self, start, range.end_bound().cloned()))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        let start = if let Some(start_key) = start_key {
            decode::<T>(self.inner.set_range(start_key.encode().as_ref()))
        } else {
            self.last()
        }
            .transpose();

        Ok(ReverseWalker::new(self, start))
    }
}

impl<K: TransactionKind, T: Table> DbCursorRO<T> for StateROCursor<K, T> {
    fn first(&mut self) -> PairResult<T> {
        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let sorted_set_key = generate_sorted_set_key::<T>();

        let res : Result<Vec<String>, DatabaseError>= connection.zrangebylex_limit(sorted_set_key, "-", "+", 0, 1).map_err(|e| DatabaseError::Read(from(e)));
        match res {
            Ok(v) => {
                if v.len() > 1 {
                    panic!("Incorrect number of value returned")
                } else if v.len() < 1 {
                    Ok(None)
                } else {
                    let key_bytes = v[0].as_bytes();
                    let redis_key = generate_redis_key_from_byte::<T>(key_bytes);
                    let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;

                    let key_value_pair:  Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, DatabaseErrorInfo> = match value {
                        Some(v) =>Ok(Some((Cow::Owned(key_bytes.to_vec()), Cow::Owned(v)))),
                        _ => Err(DatabaseError::Read(DatabaseErrorInfo{ message: "failed to get value".to_string(), code: 0 }))?
                    };

                    let decoded_res = decode::<T>(key_value_pair.clone());
                    let decoded_res_copy = decode::<T>(key_value_pair);
                    self.current_key = Some(decoded_res_copy.unwrap().unwrap().0);

                    decoded_res
                }
            }
            Err(e) => Err(e)
        }
    }

    fn seek_exact(&mut self, key: <T as Table>::Key) -> PairResult<T> {
        panic!("Not supported")
    }

    fn seek(&mut self, key: <T as Table>::Key) -> PairResult<T> {
        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let sorted_set_key = generate_sorted_set_key::<T>();

        let min: Vec<u8> = "[".as_bytes().to_vec().into_iter().chain(key.clone().encode().into().into_iter()).collect();
        let res : Result<Vec<String>, DatabaseError>= connection.zrangebylex_limit(sorted_set_key, min, "+", 0, 1).map_err(|e| DatabaseError::Read(from(e)));
        match res {
            Ok(v) => {
                if v.len() > 1 {
                    panic!("Incorrect number of value returned")
                } else if v.len() < 1 {
                    Ok(None)
                } else {
                    let key_bytes = v[0].as_bytes();
                    let redis_key = generate_redis_key_from_byte::<T>(key_bytes);
                    let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;

                    let key_value_pair:  Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, DatabaseErrorInfo> = match value {
                        Some(v) =>Ok(Some((Cow::Owned(key_bytes.to_vec()), Cow::Owned(v)))),
                        _ => Err(DatabaseError::Read(DatabaseErrorInfo{ message: "failed to get value".to_string(), code: 0 }))?
                    };

                    let decoded_res = decode::<T>(key_value_pair.clone());
                    let decoded_res_copy = decode::<T>(key_value_pair);
                    self.current_key = Some(decoded_res_copy.unwrap().unwrap().0);

                    decoded_res
                }
            }
            Err(e) => Err(e)
        }
    }

    fn next(&mut self) -> PairResult<T> {
        match self.current_key.clone() {
            Some(key) => {
                let sorted_set_key = generate_sorted_set_key::<T>();
                let min: Vec<u8> = "(".as_bytes().to_vec().into_iter().chain(key.clone().encode().into().into_iter()).collect();
                let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
                let res : Result<Vec<String>, DatabaseError>= connection.zrangebylex_limit(sorted_set_key, min, "+", 0, 1).map_err(|e| DatabaseError::Read(from(e)));

                match res {
                    Ok(v) => {
                        if v.len() > 1 {
                            panic!("Incorrect number of value returned")
                        } else if v.len() < 1 {
                            Ok(None)
                        } else {
                            let key_bytes = v[0].as_bytes();
                            let redis_key = generate_redis_key_from_byte::<T>(key_bytes);
                            let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;

                            let key_value_pair:  Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, DatabaseErrorInfo> = match value {
                                Some(v) =>Ok(Some((Cow::Owned(key_bytes.to_vec()), Cow::Owned(v)))),
                                _ => Err(DatabaseError::Read(DatabaseErrorInfo{ message: "failed to get value".to_string(), code: 0 }))?
                            };

                            let decoded_res = decode::<T>(key_value_pair.clone());
                            let decoded_res_copy = decode::<T>(key_value_pair);
                            self.current_key = Some(decoded_res_copy.unwrap().unwrap().0);

                            decoded_res
                        }
                    }
                    Err(e) => Err(e)
                }
            }
            _ => Ok(None),
        }
    }

    fn prev(&mut self) -> PairResult<T> {
        match self.current_key.clone() {
            Some(key) => {
                let sorted_set_key = generate_sorted_set_key::<T>();
                let max: Vec<u8> = "(".as_bytes().to_vec().into_iter().chain(key.clone().encode().into().into_iter()).collect();
                let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
                let res : Result<Vec<String>, DatabaseError>= connection.zrangebylex_limit(sorted_set_key, "-", max, 0, 1).map_err(|e| DatabaseError::Read(from(e)));

                match res {
                    Ok(v) => {
                        if v.len() > 1 {
                            panic!("Incorrect number of value returned")
                        } else if v.len() < 1 {
                            Ok(None)
                        } else {
                            let key_bytes = v[0].as_bytes();
                            let redis_key = generate_redis_key_from_byte::<T>(key_bytes);
                            let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;

                            let key_value_pair:  Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, DatabaseErrorInfo> = match value {
                                Some(v) =>Ok(Some((Cow::Owned(key_bytes.to_vec()), Cow::Owned(v)))),
                                _ => Err(DatabaseError::Read(DatabaseErrorInfo{ message: "failed to get value".to_string(), code: 0 }))?
                            };

                            let decoded_res = decode::<T>(key_value_pair.clone());
                            let decoded_res_copy = decode::<T>(key_value_pair);
                            self.current_key = Some(decoded_res_copy.unwrap().unwrap().0);

                            decoded_res
                        }
                    }
                    Err(e) => Err(e)
                }
            }
            _ => Ok(None),
        }
    }

    fn last(&mut self) -> PairResult<T> {
        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let sorted_set_key = generate_sorted_set_key::<T>();

        let res : Result<Vec<String>, DatabaseError>= connection.zrevrangebylex_limit(sorted_set_key, "+", "-", 0, 1).map_err(|e| DatabaseError::Read(from(e)));
        match res {
            Ok(v) => {
                if v.len() > 1 {
                    panic!("Incorrect number of value returned")
                } else if v.len() < 1 {
                    Ok(None)
                } else {
                    let key_bytes = v[0].as_bytes();
                    let redis_key = generate_redis_key_from_byte::<T>(key_bytes);
                    let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;

                    let key_value_pair:  Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, DatabaseErrorInfo> = match value {
                        Some(v) =>Ok(Some((Cow::Owned(key_bytes.to_vec()), Cow::Owned(v)))),
                        _ => Err(DatabaseError::Read(DatabaseErrorInfo{ message: "failed to get value".to_string(), code: 0 }))?
                    };

                    let decoded_res = decode::<T>(key_value_pair.clone());
                    let decoded_res_copy = decode::<T>(key_value_pair);
                    self.current_key = Some(decoded_res_copy.unwrap().unwrap().0);

                    decoded_res
                }
            }
            Err(e) => Err(e)
        }
    }

    fn current(&mut self) -> PairResult<T> {
        match self.current_key.clone() {
            Some(key) => {
                let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
                let redis_key = generate_redis_key::<T>(&key);
                let value: Option<Vec<u8>> = connection.get(redis_key).map_err(|e| DatabaseError::Read(from(e)))?;

                let key_value_pair:  Result<Option<(Cow<'_, [u8]>, Cow<'_, [u8]>)>, DatabaseErrorInfo> = match value {
                    Some(v) => {
                        Ok(Some((Cow::Owned(self.current_key.clone().unwrap().encode().as_ref().to_vec()), Cow::Owned(v))))
                    },
                    _ => Err(DatabaseError::Read(DatabaseErrorInfo{ message: "failed to get value".to_string(), code: 0 }))?
                };

                let decoded_res = decode::<T>(key_value_pair.clone());
                let decoded_res_copy = decode::<T>(key_value_pair);
                self.current_key = Some(decoded_res_copy.unwrap().unwrap().0);

                decoded_res
            }
            _ => Ok(None),
        }
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError> {
        panic!("not supported")
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError> {
        panic!("not supported")
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        panic!("not supported")
    }
}

impl<K: TransactionKind, T: DupSort> DbDupCursorRO<T> for Cursor<K, T> {
    /// Returns the next `(key, value)` pair of a DUPSORT table.
    fn next_dup(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.next_dup())
    }

    /// Returns the next `(key, value)` pair skipping the duplicates.
    fn next_no_dup(&mut self) -> PairResult<T> {
        decode::<T>(self.inner.next_nodup())
    }

    /// Returns the next `value` of a duplicate `key`.
    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        self.inner
            .next_dup()
            .map_err(|e| DatabaseError::Read(e.into()))?
            .map(decode_value::<T>)
            .transpose()
    }

    fn seek_by_key_subkey(
        &mut self,
        key: <T as Table>::Key,
        subkey: <T as DupSort>::SubKey,
    ) -> ValueOnlyResult<T> {
        self.inner
            .get_both_range(key.encode().as_ref(), subkey.encode().as_ref())
            .map_err(|e| DatabaseError::Read(e.into()))?
            .map(decode_one::<T>)
            .transpose()
    }

    /// Depending on its arguments, returns an iterator starting at:
    /// - Some(key), Some(subkey): a `key` item whose data is >= than `subkey`
    /// - Some(key), None: first item of a specified `key`
    /// - None, Some(subkey): like first case, but in the first key
    /// - None, None: first item in the table of a DUPSORT table.
    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
        let start = match (key, subkey) {
            (Some(key), Some(subkey)) => {
                // encode key and decode it after.
                let key: Vec<u8> = key.encode().into();
                self.inner
                    .get_both_range(key.as_ref(), subkey.encode().as_ref())
                    .map_err(|e| DatabaseError::Read(e.into()))?
                    .map(|val| decoder::<T>((Cow::Owned(key), val)))
            }
            (Some(key), None) => {
                let key: Vec<u8> = key.encode().into();
                self.inner
                    .set(key.as_ref())
                    .map_err(|e| DatabaseError::Read(e.into()))?
                    .map(|val| decoder::<T>((Cow::Owned(key), val)))
            }
            (None, Some(subkey)) => {
                if let Some((key, _)) = self.first()? {
                    let key: Vec<u8> = key.encode().into();
                    self.inner
                        .get_both_range(key.as_ref(), subkey.encode().as_ref())
                        .map_err(|e| DatabaseError::Read(e.into()))?
                        .map(|val| decoder::<T>((Cow::Owned(key), val)))
                } else {
                    Some(Err(DatabaseError::Read(MDBXError::NotFound.into())))
                }
            }
            (None, None) => self.first().transpose(),
        };

        Ok(DupWalker::<'_, T, Self> { cursor: self, start })
    }
}

impl<T: Table> DbCursorRW<T> for Cursor<RW, T> {
    /// Database operation that will update an existing row if a specified value already
    /// exists in a table, and insert a new row if the specified value doesn't already exist
    ///
    /// For a DUPSORT table, `upsert` will not actually update-or-insert. If the key already exists,
    /// it will append the value to the subkey, even if the subkeys are the same. So if you want
    /// to properly upsert, you'll need to `seek_exact` & `delete_current` if the key+subkey was
    /// found, before calling `upsert`.
    fn upsert(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let key_copy = key.clone();
        let set_key = key.clone();
        let key = key.encode();
        let value = compress_to_buf_or_ref!(self, value);

        let redis_key = generate_redis_key::<T>(&key_copy);
        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let _: RedisResult<()> = connection.set(redis_key.clone(), value.unwrap_or(&self.buf));

        let sorted_set_key = generate_sorted_set_key::<T>();
        let redis_res: RedisResult<Value> = connection.zadd(sorted_set_key, set_key.clone().encode().as_ref(), 0);
        self.current_key = Some(set_key.clone());

        self.execute_with_operation_metric(
            Operation::CursorUpsert,
            Some(value.unwrap_or(&self.buf).len()),
            |this| {
                this.inner
                    .put(key.as_ref(), value.unwrap_or(&this.buf), WriteFlags::UPSERT)
                    .map_err(|e| {
                        DatabaseWriteError {
                            info: e.into(),
                            operation: DatabaseWriteOperation::CursorUpsert,
                            table_name: T::NAME,
                            key: key.into(),
                        }
                            .into()
                    })
            },
        )
    }

    fn insert(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let key_copy = key.clone();
        let set_key = key.clone();
        let key = key.encode();
        let value = compress_to_buf_or_ref!(self, value);

        let redis_key = generate_redis_key::<T>(&key_copy);
        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let stored_value: Option<Vec<u8>> = connection.get(redis_key.clone()).map_err(|e| DatabaseError::Read(from(e)))?;
        if let None = stored_value {
            let _: RedisResult<()> = connection.set(redis_key.clone(), value.unwrap_or(&self.buf));
            let sorted_set_key = generate_sorted_set_key::<T>();
            let _: RedisResult<Value> = connection.zadd(sorted_set_key, set_key.clone().encode().as_ref(), 0);
            self.current_key = Some(set_key.clone())
        }

        self.execute_with_operation_metric(
            Operation::CursorInsert,
            Some(value.unwrap_or(&self.buf).len()),
            |this| {
                this.inner
                    .put(key.as_ref(), value.unwrap_or(&this.buf), WriteFlags::NO_OVERWRITE)
                    .map_err(|e| {
                        DatabaseWriteError {
                            info: e.into(),
                            operation: DatabaseWriteOperation::CursorInsert,
                            table_name: T::NAME,
                            key: key.into(),
                        }
                            .into()
                    })
            },
        )
    }

    /// Appends the data to the end of the table. Consequently, the append operation
    /// will fail if the inserted key is less than the last table key
    fn append(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let key_copy = key.clone();
        let set_key = key.clone();
        let key = key.encode();

        let redis_key = generate_redis_key::<T>(&key_copy);
        let value = compress_to_buf_or_ref!(self, value);
        self.execute_with_operation_metric(
            Operation::CursorAppend,
            Some(value.unwrap_or(&self.buf).len()),
            |this| {
                this.inner
                    .put(key.as_ref(), value.unwrap_or(&this.buf), WriteFlags::APPEND)
                    .map_err(|e| {
                        DatabaseWriteError {
                            info: e.into(),
                            operation: DatabaseWriteOperation::CursorAppend,
                            table_name: T::NAME,
                            key: key.into(),
                        }
                            // .into()
                    })
            },
        )?;

        let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
        let sorted_set_key = generate_sorted_set_key::<T>();
        let res: RedisResult<()> = connection.zadd(sorted_set_key, set_key.clone().encode().as_ref(), 0);
        self.current_key = Some(set_key.clone());
        res.map_err(|e| DatabaseError::Open(from(e)))
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if self.current_key != None {
            let mut connection = self.redis.get_connection().map_err(|e| DatabaseError::Open(from(e))).unwrap();
            let sorted_set_key = generate_sorted_set_key::<T>();
            let set_key = self.current_key.clone().unwrap();
            let res: RedisResult<()> = connection.zrem(sorted_set_key, set_key.encode().as_ref());
            let test = self.current_key.clone().unwrap();
            let redis_key = generate_redis_key::<T>(&self.current_key.clone().unwrap());
            let _: RedisResult<()> = connection.del(redis_key);
            self.current_key = None;
        }

        self.execute_with_operation_metric(Operation::CursorDeleteCurrent, None, |this| {
            this.inner.del(WriteFlags::CURRENT).map_err(|e| DatabaseError::Delete(e.into()))
        })
    }
}

fn generate_redis_key<T: Table>(key: &T::Key) -> Vec<u8> {
    let prefix = format!("{}:", T::NAME);
    let encoded_key = key.clone().encode();
    let encoded_key= encoded_key.as_ref();
    [prefix.encode().as_slice(), encoded_key].concat()
}

fn generate_redis_key_from_byte<T: Table>(key: &[u8]) -> Vec<u8> {
    let prefix = format!("{}:", T::NAME);
    [prefix.encode().as_slice(), key].concat()
}

fn generate_sorted_set_key<T: Table>() -> String {
   format!("sorted:{}",T::NAME)
}

impl<T: DupSort> DbDupCursorRW<T> for Cursor<RW, T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        self.execute_with_operation_metric(Operation::CursorDeleteCurrentDuplicates, None, |this| {
            this.inner.del(WriteFlags::NO_DUP_DATA).map_err(|e| DatabaseError::Delete(e.into()))
        })
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let key = key.encode();
        let value = compress_to_buf_or_ref!(self, value);
        self.execute_with_operation_metric(
            Operation::CursorAppendDup,
            Some(value.unwrap_or(&self.buf).len()),
            |this| {
                this.inner
                    .put(key.as_ref(), value.unwrap_or(&this.buf), WriteFlags::APPEND_DUP)
                    .map_err(|e| {
                        DatabaseWriteError {
                            info: e.into(),
                            operation: DatabaseWriteOperation::CursorAppendDup,
                            table_name: T::NAME,
                            key: key.into(),
                        }
                            .into()
                    })
            },
        )
    }
}
