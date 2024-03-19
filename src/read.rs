use super::*;

/// A read only transaction over the [`EquivalentDB`],
pub struct ReadTransaction<K, V, I, C> {
  db: I,
  rtm: Rtm<K, V, C, PendingMap<K, V>>,
}

impl<K, V, I, C> ReadTransaction<K, V, I, C> {
  #[inline]
  pub(super) fn new(db: I, rtm: Rtm<K, V, C, PendingMap<K, V>>) -> Self {
    Self { db, rtm }
  }
}

impl<K, V, I, C> ReadTransaction<K, V, I, C>
where
  K: Ord,
  I: Database<K, V>,
{
  /// Get a value from the database.
  #[inline]
  pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().get(key, version)
  }

  /// Returns true if the given key exists in the database.
  #[inline]
  pub fn contains_key<Q>(&self, key: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().contains_key(key, version)
  }

  /// Get all the values in different versions for the given key.
  #[inline]
  pub fn get_all_versions<'a, 'b: 'a, Q>(&'a self, key: &'b Q) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let version = self.rtm.version();
    self.db.as_inner().get_all_versions(key, version)
  }

  /// Returns an iterator over the entries of the database.
  #[inline]
  pub fn iter(&self) -> Iter<'_, K, V> {
    let version = self.rtm.version();
    self.db.as_inner().iter(version)
  }

  /// Returns an iterator over the entries (all versions, including removed one) of the database.
  #[inline]
  pub fn iter_all_versions(&self) -> AllVersionsIter<'_, K, V> {
    let version = self.rtm.version();
    self.db.as_inner().iter_all_versions(version)
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
    self.db.as_inner().range(range, version)
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
    self.db.as_inner().range_all_versions(range, version)
  }
}
