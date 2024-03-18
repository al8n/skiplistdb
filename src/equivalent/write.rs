use mwmr::error::WtmError;

use self::{
  iter::{WriteTransactionAllVersions, WriteTransactionAllVersionsIter, WriteTransactionIter},
  range::{WriteTransactionAllVersionsRange, WriteTransactionRange},
};

use super::*;

/// A read only transaction over the [`EquivalentDB`],
pub struct WriteTransaction<K, V, S> {
  db: EquivalentDB<K, V, S>,
  wtm: Wtm<K, V, HashCm<K, S>, PendingMap<K, V>>,
}

impl<K, V, S> WriteTransaction<K, V, S>
where
  K: Ord + core::hash::Hash + Eq + 'static,
  V: 'static,
  S: BuildHasher + Clone + 'static,
{
  #[inline]
  pub(super) fn new(db: EquivalentDB<K, V, S>) -> Self {
    let wtm = db
      .inner
      .tm
      .write(
        Options::default()
          .with_max_batch_entries(db.inner.max_batch_entries)
          .with_max_batch_size(db.inner.max_batch_size),
        Some(db.inner.hasher.clone()),
      )
      .unwrap();
    Self { db, wtm }
  }
}

impl<K, V, S> WriteTransaction<K, V, S>
where
  K: Ord + core::hash::Hash + Eq + 'static,
  V: Send + 'static,
  S: BuildHasher + 'static,
{
  /// Commits the transaction, following these steps:
  ///
  /// 1. If there are no writes, return immediately.
  ///
  /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
  ///
  /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
  ///
  /// 4. Batch up all writes, write them to database.
  ///
  /// 5. If callback is provided, Badger will return immediately after checking
  /// for conflicts. Writes to the database will happen in the background.  If
  /// there is a conflict, an error will be returned and the callback will not
  /// run. If there are no conflicts, the callback will be called in the
  /// background upon successful completion of writes or any error during write.
  #[inline]
  pub fn commit(
    &mut self,
  ) -> Result<(), WtmError<HashCm<K, S>, PendingMap<K, V>, core::convert::Infallible>> {
    self.wtm.commit(|ents| {
      self.db.apply(ents);
      Ok(())
    })
  }
}

impl<K, V, S> WriteTransaction<K, V, S>
where
  K: Ord + core::hash::Hash + Eq + Send + Sync + 'static,
  V: Send + Sync + 'static,
  S: BuildHasher + Send + Sync + 'static,
{
  /// Acts like [`commit`](WriteTransaction::commit), but takes a callback, which gets run via a
  /// thread to avoid blocking this function. Following these steps:
  ///
  /// 1. If there are no writes, return immediately, callback will be invoked.
  ///
  /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
  ///
  /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
  ///
  /// 4. Batch up all writes, write them to database.
  ///
  /// 5. Return immediately after checking for conflicts.
  /// If there is a conflict, an error will be returned immediately and the callback will not
  /// run. If there are no conflicts, the callback will be called in the
  /// background upon successful completion of writes or any error during write.
  #[inline]
  pub fn commit_with_callback<F, E, R>(
    &mut self,
    callback: impl FnOnce(Result<(), E>) -> R + Send + 'static,
  ) -> Result<std::thread::JoinHandle<R>, WtmError<HashCm<K, S>, PendingMap<K, V>, E>>
  where
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Result<(), E> + Send + 'static,
    E: std::error::Error,
    R: Send + 'static,
  {
    let db = self.db.clone();

    self.wtm.commit_with_callback(
      move |ents| {
        db.apply(ents);
        Ok(())
      },
      callback,
    )
  }
}

#[cfg(feature = "future")]
impl<K, V, H> WriteTransaction<K, V, H>
where
  K: Ord + core::hash::Hash + Eq + Send + Sync + 'static,
  V: Send + Sync + 'static,
  H: BuildHasher + Send + Sync + 'static,
{
  /// Acts like [`commit`](WriteTransaction::commit), but takes a future and a spawner, which gets run via a
  /// task to avoid blocking this function. Following these steps:
  ///
  /// 1. If there are no writes, return immediately, a new task will be spawned, and future will be invoked.
  ///
  /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
  ///
  /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
  ///
  /// 4. Batch up all writes, write them to database.
  ///
  /// 5. Return immediately after checking for conflicts.
  /// If there is a conflict, an error will be returned immediately and the no task will be spawned
  /// run. If there are no conflicts, a task will be spawned and the future will be called in the
  /// background upon successful completion of writes or any error during write.
  #[cfg_attr(docsrs, doc(cfg(feature = "future")))]
  #[inline]
  pub fn commit_with_task<F, E, R, AS>(
    &mut self,
    fut: impl FnOnce(Result<(), E>) -> R + Send + 'static,
  ) -> Result<<AS as mwmr::AsyncSpawner>::JoinHandle<R>, WtmError<HashCm<K, H>, PendingMap<K, V>, E>>
  where
    F: FnOnce(OneOrMore<Entry<K, V>>) -> Result<(), E> + Send + 'static,
    E: std::error::Error,
    R: Send + 'static,
    AS: mwmr::AsyncSpawner,
  {
    let db = self.db.clone();

    self.wtm.commit_with_task::<_, _, _, AS>(
      move |ents| {
        db.apply(ents);
        Ok(())
      },
      fut,
    )
  }
}

impl<K, V, S> WriteTransaction<K, V, S>
where
  K: Ord + core::hash::Hash + Eq + 'static,
  V: 'static,
  S: BuildHasher + 'static,
{
  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(
    &mut self,
    key: &Q,
  ) -> Result<bool, TransactionError<HashCm<K, S>, PendingMap<K, V>>>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized,
  {
    let version = self.wtm.version();
    match self.wtm.contains_key_equivalent_cm_comparable_pm(key)? {
      Some(true) => Ok(true),
      Some(false) => Ok(false),
      None => Ok(self.db.contains_key(key, version)),
    }
  }

  /// Get a value from the database.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<
    Option<Either<PendingRef<'a, K, V>, Ref<'a, K, V>>>,
    TransactionError<HashCm<K, S>, PendingMap<K, V>>,
  >
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized,
  {
    let version = self.wtm.version();
    match self.wtm.get_equivalent_cm_comparable_pm(key)? {
      Some(v) => {
        if v.value().is_some() {
          Ok(Some(Either::Left(PendingRef::new(v))))
        } else {
          Ok(None)
        }
      }
      None => Ok(self.db.get(key, version).map(Either::Right)),
    }
  }

  /// Get all the values in different versions for the given key. Including the removed ones.
  #[inline]
  pub fn get_all_versions<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<
    Option<WriteTransactionAllVersions<'a, K, V>>,
    TransactionError<HashCm<K, S>, PendingMap<K, V>>,
  >
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Ord + ?Sized,
  {
    let version = self.wtm.version();
    let mut pending = None;
    if let Some(ent) = self.wtm.get_equivalent_cm_comparable_pm(key)? {
      pending = Some(OptionPendingRef::new(ent));
    }

    let committed = self.db.get_all_versions(key, version);

    if committed.is_none() && pending.is_none() {
      return Ok(None);
    }
    Ok(Some(WriteTransactionAllVersions { committed, pending }))
  }

  /// Insert a new key-value pair.
  #[inline]
  pub fn insert(
    &mut self,
    key: K,
    value: V,
  ) -> Result<(), TransactionError<HashCm<K, S>, PendingMap<K, V>>> {
    self.wtm.insert(key, value)
  }

  /// Remove a key.
  #[inline]
  pub fn remove(&mut self, key: K) -> Result<(), TransactionError<HashCm<K, S>, PendingMap<K, V>>> {
    self.wtm.remove(key)
  }

  /// Iterate over the entries of the write transaction.
  #[inline]
  pub fn iter(
    &mut self,
  ) -> Result<WriteTransactionIter<'_, K, V, S>, TransactionError<HashCm<K, S>, PendingMap<K, V>>>
  {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm()?;

    let committed = self.db.iter(version);
    let pendings = pm.map.iter();

    Ok(WriteTransactionIter::new(pendings, committed, marker))
  }

  /// Returns an iterator over the entries (all versions, including removed one) of the database.
  #[inline]
  pub fn iter_all_versions(
    &mut self,
  ) -> Result<
    WriteTransactionAllVersionsIter<'_, K, V, S>,
    TransactionError<HashCm<K, S>, PendingMap<K, V>>,
  > {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm()?;

    let committed = self.db.iter_all_versions(version);
    let pendings = pm.map.iter();

    Ok(WriteTransactionAllVersionsIter::new(
      &self.db, version, pendings, committed, marker,
    ))
  }

  /// Returns an iterator over the subset of entries of the database.
  #[inline]
  pub fn range<'a, Q, R>(
    &'a mut self,
    range: R,
  ) -> Result<
    WriteTransactionRange<'a, Q, R, K, V, S>,
    TransactionError<HashCm<K, S>, PendingMap<K, V>>,
  >
  where
    K: Borrow<Q>,
    R: RangeBounds<Q> + 'a,
    Q: Ord + ?Sized,
  {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm()?;
    let start = range.start_bound();
    let end = range.end_bound();
    let pendings = pm.map.range((start, end));
    let committed = self.db.range(range, version);

    Ok(WriteTransactionRange::new(pendings, committed, marker))
  }

  /// Returns an iterator over the subset of entries (all versions, including removed one) of the database.
  #[inline]
  pub fn range_all_versions<'a, Q, R>(
    &'a mut self,
    range: R,
  ) -> Result<
    WriteTransactionAllVersionsRange<'a, Q, R, K, V, S>,
    TransactionError<HashCm<K, S>, PendingMap<K, V>>,
  >
  where
    K: Borrow<Q>,
    R: RangeBounds<Q> + 'a,
    Q: Ord + ?Sized,
  {
    let version = self.wtm.version();
    let (marker, pm) = self.wtm.marker_with_pm()?;
    let start = range.start_bound();
    let end = range.end_bound();
    let pendings = pm.map.range((start, end));
    let committed = self.db.range_all_versions(range, version);

    Ok(WriteTransactionAllVersionsRange::new(
      &self.db, version, pendings, committed, marker,
    ))
  }
}
