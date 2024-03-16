use std::hash::BuildHasher;

use mwmr::{Cm, EntryDataRef, EntryRef};

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
  pub fn get<'a, 'b: 'a, Q>(&'a mut self, key: &'b Q) -> Result<Option<Either<&'a V, Arc<V>>>, ()>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized,
  {
    if self.wtm.is_discard() {
      return Err(());
    }

    if self.wtm.pwm().unwrap().contains_by_borrow(key) {
      let e = self.wtm.pwm().unwrap().get_by_borrow(key).unwrap();
      // If the value is None, it means that the key is removed.
      return match e.value {
        Some(ref value) => Ok(Some(Either::Left(value))),
        None => Ok(None),
      };
    }

    // track reads. No need to track read if txn serviced it
    // internally.
    let cm = self.wtm.cm_mut().unwrap();
    cm.mark_read_borrow(key);

    let version = self.wtm.version();

    Ok(self.db.get(key, version).map(|v| Either::Right(v)))
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(&mut self, key: &Q) -> Result<bool, ()>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Eq + Ord + ?Sized,
  {
    if self.wtm.is_discard() {
      return Err(());
    }

    if self.wtm.pwm().unwrap().contains_by_borrow(key) {
      return Ok(true);
    }

    // track reads. No need to track read if txn serviced it
    // internally.
    let cm = self.wtm.cm_mut().unwrap();
    cm.mark_read_borrow(key);

    let version = self.wtm.version();

    Ok(self.db.contains_key(key, version))
  }

  /// Get all the values in different versions for the given key.
  #[inline]
  pub fn get_all_versions<'a, 'b: 'a, Q>(
    &'a self,
    key: &'b Q,
  ) -> Result<Option<AllVersionsWithPending<'a, K, V>>, ()>
  where
    K: Borrow<Q>,
    Q: core::hash::Hash + Ord + ?Sized,
  {
    if self.wtm.is_discard() {
      return Err(());
    }

    let mut pending = None;
    if self.wtm.pwm().unwrap().contains_by_borrow(key) {
      let e = self.wtm.pwm().unwrap().get_by_borrow(key).unwrap();
      // If the value is None, it means that the key is removed.
      match e.value {
        Some(ref value) => {
          pending = Some(value.clone());
        }
        None => {}
      };
    }

    // track reads. No need to track read if txn serviced it
    // internally.
    let cm = self.wtm.cm_mut().unwrap();
    cm.mark_read_borrow(key);

    let version = self.wtm.version();

    Ok(self.db.get(key, version).map(|v| Either::Right(v)))
  }
}
