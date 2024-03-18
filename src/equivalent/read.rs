use super::*;

/// A read only transaction over the [`EquivalentDB`],
pub struct ReadTransaction<K, V, S> {
  db: EquivalentDB<K, V, S>,
  rtm: Rtm<K, V, HashCm<K, S>, PendingMap<K, V>>,
}

impl<K, V, S> ReadTransaction<K, V, S> {
  #[inline]
  pub(super) fn new(db: EquivalentDB<K, V, S>) -> Self {
    let rtm = db.inner.tm.read();
    Self { db, rtm }
  }
}

impl<K, V, S> ReadTransaction<K, V, S>
where
  K: Ord,
{
  /// Get a value from the database.
  #[inline]
  pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.get(key, version)
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.contains_key(key, version)
  }

  /// Get all the values in different versions for the given key.
  #[inline]
  pub fn get_all_versions<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.get_all_versions(key, version)
  }

  /// Returns an iterator over the entries of the database.
  #[inline]
  pub fn iter(&self) -> Iter<'_, K, V> {
    let version = self.rtm.version();
    self.db.iter(version)
  }

  /// Returns an iterator over the entries (all versions, including removed one) of the database.
  #[inline]
  pub fn iter_all_versions(&self) -> AllVersionsIter<'_, K, V> {
    let version = self.rtm.version();
    self.db.iter_all_versions(version)
  }

  /// Returns an iterator over the subset of entries of the database.
  #[inline]
  pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.range(range, version)
  }

  /// Returns an iterator over the subset of entries (all versions, including removed one) of the database.
  #[inline]
  pub fn range_all_versions<Q, R>(&self, range: R) -> AllVersionsRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.range_all_versions(range, version)
  }
}

impl<K, V, S> Clone for ReadTransaction<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self::new(self.db.clone())
  }
}
