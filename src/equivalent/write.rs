use std::hash::BuildHasher;

use mwmr::{Cm, EntryDataRef};

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
  V: 'static,
  S: BuildHasher + 'static,
{
  /// Get a value from the database.
  #[inline]
  pub fn get<'a, 'b: 'a, Q>(&'a mut self, key: &'b Q) -> Result<Option<Either<EntryRef<'a, K, V>, Arc<V>>>, TransactionError<HashCm<K, S>, PendingMap<K, V>>>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized,
  {
    let version = self.wtm.version();
    match self.wtm.get_equivalent_cm_comparable_pm(key)? {
      Some(v) => if v.value().is_some() {
        return Ok(Some(Either::Left(EntryRef::new(v))));
      } else {
        return Ok(None);
      },
      None => {
        Ok(self.db.get(key, version).map(|v| Either::Right(v)))
      },
    }
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(&mut self, key: &Q) -> Result<bool, TransactionError<HashCm<K, S>, PendingMap<K, V>>>
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

  /// Get all the values in different versions for the given key. Including the removed ones.
  #[inline]
  pub fn get_all_versions<'a, 'b: 'a, Q>(
    &'a mut self,
    key: &'b Q,
  ) -> Result<Option<AllVersionsWithPending<'a, K, V>>, TransactionError<HashCm<K, S>, PendingMap<K, V>>>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Ord + ?Sized,
  {
    let version = self.wtm.version();
    let mut pending = None;
    if let Some(ent) = self.wtm.get_equivalent_cm_comparable_pm(key)? {
      pending = Some(OptionEntryRef::new(ent));
    }

    let committed = self.db.get_all_versions(key, version);

    if committed.is_none() && pending.is_none() {
      return Ok(None);
    }
    Ok(Some(AllVersionsWithPending { committed, pending }))
  }
}
