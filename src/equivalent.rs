use super::*;

use std::{cmp, collections::btree_map::Iter as BTreeMapIter};

mod read;
use mwmr::Marker;
pub use read::*;

mod write;
pub use write::*;

/// A reference to an entry in the write transaction.
pub struct Entry<'a, K, V> {
  ent: MapEntry<'a, u64, Option<V>>,
  key: &'a K,
  version: u64,
}

impl<'a, K, V> Clone for Entry<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
      key: self.key,
    }
  }
}

impl<'a, K, V> Entry<'a, K, V> {
  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> &V {
    self.ent.value().as_ref().unwrap()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.key
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct Ref<'a, K, V> {
  ent: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, K, V> Clone for Ref<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> Ref<'a, K, V> {
  /// Get the value of the entry.
  #[inline]
  pub fn entry(&self) -> Entry<'_, K, V> {
    let ent = self.ent.value().get(&self.version).unwrap();

    Entry {
      ent,
      key: self.ent.key(),
      version: self.version,
    }
  }

  /// Get the key of the ref.
  #[inline]
  pub fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct OptionRef<'a, K, V> {
  ent: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, K, V> Clone for OptionRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      version: self.version,
    }
  }
}

impl<'a, K, V> OptionRef<'a, K, V> {
  /// Get the value of the entry. `None` this entry is marked as removed.
  #[inline]
  pub fn entry(&self) -> Option<Entry<'_, K, V>> {
    let ent = self.ent.value().get(&self.version).unwrap();

    if ent.value().is_some() {
      return Some(Entry {
        ent,
        key: self.ent.key(),
        version: self.version,
      });
    }

    None
  }

  /// Get the key of the ref.
  #[inline]
  pub fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

/// A reference to an entry in the write transaction.
pub struct PendingRef<'a, K, V> {
  ent: mwmr::EntryRef<'a, K, V>,
}

impl<'a, K, V> Clone for PendingRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, K, V> Copy for PendingRef<'a, K, V> {}

impl<'a, K, V> PendingRef<'a, K, V> {
  #[inline]
  const fn new(ent: mwmr::EntryRef<'a, K, V>) -> Self {
    Self { ent }
  }

  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> &V {
    self.ent.value().unwrap()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.ent.version()
  }
}

/// A reference to an entry in the write transaction. If the value is None, it means that the key is removed.
pub struct OptionPendingRef<'a, K, V> {
  ent: mwmr::EntryRef<'a, K, V>,
}

impl<'a, K, V> Clone for OptionPendingRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, K, V> Copy for OptionPendingRef<'a, K, V> {}

impl<'a, K, V> OptionPendingRef<'a, K, V> {
  /// Create a new entry reference.
  #[inline]
  const fn new(ent: mwmr::EntryRef<'a, K, V>) -> Self {
    Self { ent }
  }

  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> Option<&V> {
    self.ent.value()
  }

  /// Get the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.ent.version()
  }
}

struct Inner<K, V, S = std::hash::DefaultHasher> {
  tm: Tm<K, V, HashCm<K, S>, PendingMap<K, V>>,
  map: SkipMap<K, SkipMap<u64, Option<V>>>,
  hasher: S,
  max_batch_size: u64,
  max_batch_entries: u64,
}

impl<K, V, S> Inner<K, V, S> {
  fn new(name: &str, max_batch_size: u64, max_batch_entries: u64, hasher: S) -> Self {
    let map = SkipMap::new();
    let tm = Tm::new(name, 0);
    Self {
      tm,
      map,
      hasher,
      max_batch_size,
      max_batch_entries,
    }
  }
}

/// A concurrent ACID, MVCC in-memory database based on [`crossbeam-skiplist`][crossbeam_skiplist].
pub struct EquivalentDB<K, V, S = std::hash::DefaultHasher> {
  inner: Arc<Inner<K, V, S>>,
}

impl<K, V, S> Clone for EquivalentDB<K, V, S> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<K, V> Default for EquivalentDB<K, V> {
  /// Creates a new `EquivalentDB` with the default options.
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> EquivalentDB<K, V> {
  /// Creates a new `EquivalentDB` with the given options.
  #[inline]
  pub fn new() -> Self {
    Self::with_options_and_hasher(Default::default(), Default::default())
  }
}

impl<K, V, S> EquivalentDB<K, V, S> {
  /// Creates a new `EquivalentDB` with the given hasher.
  #[inline]
  pub fn with_hasher(hasher: S) -> Self {
    Self::with_options_and_hasher(Default::default(), hasher)
  }

  /// Creates a new `EquivalentDB` with the given options and hasher.
  #[inline]
  pub fn with_options_and_hasher(opts: Options, hasher: S) -> Self {
    let inner = Arc::new(Inner::new(
      "skiplistdb",
      opts.max_batch_size,
      opts.max_batch_entries,
      hasher,
    ));
    Self { inner }
  }

  /// Create a read transaction.
  #[inline]
  pub fn read(&self) -> ReadTransaction<K, V, S> {
    ReadTransaction::new(self.clone())
  }
}

impl<K, V, S> EquivalentDB<K, V, S>
where
  K: Ord + Eq + core::hash::Hash + 'static,
  V: 'static,
  S: BuildHasher + Clone + 'static,
{
  /// Create a write transaction.
  #[inline]
  pub fn write(&self) -> WriteTransaction<K, V, S> {
    WriteTransaction::new(self.clone())
  }
}

impl<K, V, S> EquivalentDB<K, V, S>
where
  K: Ord,
{
  fn get<Q>(&self, key: &Q, version: u64) -> Option<Ref<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.inner.map.get(key)?;
    let version = ent
      .value()
      .upper_bound(Bound::Included(&version))
      .and_then(|v| {
        if v.value().is_some() {
          Some(*v.key())
        } else {
          None
        }
      })?;

    Some(Ref { ent, version })
  }

  fn contains_key<Q>(&self, key: &Q, version: u64) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    match self.inner.map.get(key) {
      None => false,
      Some(values) => values
        .value()
        .upper_bound(Bound::Included(&version))
        .is_some(),
    }
  }

  fn get_all_versions<'a, 'b: 'a, Q>(
    &'a self,
    key: &'b Q,
    version: u64,
  ) -> Option<AllVersions<'a, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    self.inner.map.get(key).and_then(move |values| {
      let ents = values.value();
      if ents.is_empty() {
        return None;
      }

      let min = *ents.front().unwrap().key();
      if min > version {
        return None;
      }

      Some(AllVersions {
        max_version: version,
        min_version: min,
        current_version: version,
        entries: values,
      })
    })
  }

  fn iter(&self, version: u64) -> Iter<'_, K, V> {
    let iter = self.inner.map.iter();
    Iter { iter, version }
  }

  fn iter_all_versions(&self, version: u64) -> AllVersionsIter<'_, K, V> {
    let iter = self.inner.map.iter();
    AllVersionsIter { iter, version }
  }

  fn range<Q, R>(&self, range: R, version: u64) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    Range {
      range: self.inner.map.range(range),
      version,
    }
  }

  fn range_all_versions<Q, R>(&self, range: R, version: u64) -> AllVersionsRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    AllVersionsRange {
      range: self.inner.map.range(range),
      version,
    }
  }
}

/// An iterator over a all values with the same key in different versions.
pub struct AllVersions<'a, K, V> {
  max_version: u64,
  min_version: u64,
  current_version: u64,
  entries: MapEntry<'a, K, SkipMap<u64, Option<V>>>,
}

impl<'a, K, V> Clone for AllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      max_version: self.max_version,
      min_version: self.min_version,
      current_version: self.current_version,
      entries: self.entries.clone(),
    }
  }
}

impl<'a, K, V> Iterator for AllVersions<'a, K, V> {
  type Item = OptionRef<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    self
      .entries
      .value()
      .upper_bound(Bound::Included(&self.current_version))
      .map(|ent| {
        let ent_version = *ent.key();

        if self.current_version != ent_version {
          self.current_version = ent_version;
        } else {
          self.current_version += 1;
        }

        OptionRef {
          ent: self.entries.clone(),
          version: ent_version,
        }
      })
  }

  fn last(mut self) -> Option<Self::Item>
  where
    Self: Sized,
  {
    self
      .entries
      .value()
      .lower_bound(Bound::Included(&self.max_version))
      .map(|ent| {
        self.current_version = *ent.key();

        OptionRef {
          ent: self.entries.clone(),
          version: self.current_version,
        }
      })
  }
}

impl<'a, K, V> FusedIterator for AllVersions<'a, K, V> {}

/// An iterator over a all values with the same key in different versions.
pub struct WriteTransactionAllVersions<'a, K, V> {
  pending: Option<OptionPendingRef<'a, K, V>>,
  committed: Option<AllVersions<'a, K, V>>,
}

impl<'a, K, V> Clone for WriteTransactionAllVersions<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      pending: self.pending,
      committed: self.committed.clone(),
    }
  }
}

impl<'a, K, V> Iterator for WriteTransactionAllVersions<'a, K, V> {
  type Item = Either<OptionPendingRef<'a, K, V>, OptionRef<'a, K, V>>;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(p) = self.pending.take() {
      return Some(Either::Left(p));
    }

    if let Some(committed) = &mut self.committed {
      committed.next().map(Either::Right)
    } else {
      None
    }
  }

  fn last(mut self) -> Option<Self::Item>
  where
    Self: Sized,
  {
    if let Some(committed) = self.committed.take() {
      return committed.last().map(Either::Right);
    }

    self.pending.take().map(Either::Left)
  }
}

impl<'a, K, V> FusedIterator for WriteTransactionAllVersions<'a, K, V> {}

/// An iterator over the entries of the database.
pub struct Iter<'a, K, V> {
  iter: crossbeam_skiplist::map::Iter<'a, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
  K: Ord,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(Ref { version, ent });
      }
    }
  }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V>
where
  K: Ord,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next_back()?;
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(Ref { version, ent });
      }
    }
  }
}

/// Iterator over the entries of the write transaction.
pub struct WriteTransactionIter<'a, K, V, S> {
  pendings: BTreeMapIter<'a, K, EntryValue<V>>,
  committed: Iter<'a, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<Ref<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, Ref<'a, K, V>>>,
  marker: Option<Marker<'a, HashCm<K, S>>>,
}

impl<'a, K, V, S> WriteTransactionIter<'a, K, V, S>
where
  S: BuildHasher + 'static,
  K: Ord + core::hash::Hash + Eq + 'static,
{
  fn advance_pending(&mut self) {
    self.next_pending = self.pendings.next();
  }

  fn advance_committed(&mut self) {
    self.next_committed = self.committed.next();
    if let (Some(item), Some(marker)) = (&self.next_committed, &mut self.marker) {
      marker.mark(item.key());
    }
  }

  fn new(
    pendings: BTreeMapIter<'a, K, EntryValue<V>>,
    committed: Iter<'a, K, V>,
    marker: Option<Marker<'a, HashCm<K, S>>>,
  ) -> Self {
    let mut iterator = WriteTransactionIter {
      pendings,
      committed,
      next_pending: None,
      next_committed: None,
      last_yielded_key: None,
      marker,
    };

    iterator.advance_pending();
    iterator.advance_committed();

    iterator
  }
}

impl<'a, K, V, S> Iterator for WriteTransactionIter<'a, K, V, S>
where
  K: Ord + core::hash::Hash + Eq + 'static,
  S: BuildHasher + 'static,
{
  type Item = Either<(&'a K, &'a V), Ref<'a, K, V>>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      match (self.next_pending, &self.next_committed) {
        // Both pending and committed iterators have items to yield.
        (Some((pending_key, _)), Some(committed)) => {
          match pending_key.cmp(committed.key()) {
            // Pending item has a smaller key, so yield this one.
            cmp::Ordering::Less => {
              let (key, value) = self.next_pending.take().unwrap();
              self.advance_pending();
              self.last_yielded_key = Some(Either::Left(key));
              match &value.value {
                Some(value) => return Some(Either::Left((key, value))),
                None => continue,
              }
            }
            // Keys are equal, so we prefer the pending item and skip the committed one.
            cmp::Ordering::Equal => {
              // Skip committed if it has the same key as pending
              self.advance_committed();
              // Loop again to check the next item without yielding anything this time.
              continue;
            }
            // Committed item has a smaller key, so we consider yielding this one.
            cmp::Ordering::Greater => {
              let committed = self.next_committed.take().unwrap();
              self.advance_committed(); // Prepare the next committed item for future iterations.
                                        // Yield the committed item if it has not been yielded before.
              if self.last_yielded_key.as_ref().map_or(true, |k| match k {
                Either::Left(k) => *k != committed.key(),
                Either::Right(item) => item.key() != committed.key(),
              }) {
                self.last_yielded_key = Some(Either::Right(committed.clone()));
                return Some(Either::Right(committed));
              }
            }
          }
        }
        // Only pending items are left, so yield the next pending item.
        (Some((_, _)), None) => {
          let (key, value) = self.next_pending.take().unwrap();
          self.advance_pending(); // Advance the pending iterator for the next iteration.
          self.last_yielded_key = Some(Either::Left(key)); // Update the last yielded key.
          match &value.value {
            Some(value) => return Some(Either::Left((key, value))),
            None => continue,
          }
        }
        // Only committed items are left, so yield the next committed item if it hasn't been yielded already.
        (None, Some(committed)) => {
          if self.last_yielded_key.as_ref().map_or(true, |k| match k {
            Either::Left(k) => *k != committed.key(),
            Either::Right(item) => item.key() != committed.key(),
          }) {
            let committed = self.next_committed.take().unwrap();
            self.advance_committed(); // Advance the committed iterator for the next iteration.
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(Either::Right(committed));
          } else {
            // The key has already been yielded, so move to the next.
            self.advance_committed();
            // Loop again to check the next item without yielding anything this time.
            continue;
          }
        }
        // Both iterators have no items left to yield.
        (None, None) => return None,
      }
    }
  }
}

/// An iterator over the entries of the database.
pub struct AllVersionsIter<'a, K, V> {
  iter: crossbeam_skiplist::map::Iter<'a, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, K, V> Iterator for AllVersionsIter<'a, K, V>
where
  K: Ord,
{
  type Item = AllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          current_version: version,
          entries: ent,
        });
      }
    }
  }
}

impl<'a, K, V> DoubleEndedIterator for AllVersionsIter<'a, K, V>
where
  K: Ord,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.iter.next_back()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          current_version: version,
          entries: ent,
        });
      }
    }
  }
}

/// An iterator over a subset of entries of the database.
pub struct Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  range: MapRange<'a, Q, R, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, Q, R, K, V> Iterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = Ref<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next()?;
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(Ref { version, ent });
      }
    }
  }
}

impl<'a, Q, R, K, V> DoubleEndedIterator for Range<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next_back()?;
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(Ref { version, ent });
      }
    }
  }
}

/// An iterator over a subset of entries of the database.
pub struct AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  range: MapRange<'a, Q, R, K, SkipMap<u64, Option<V>>>,
  version: u64,
}

impl<'a, Q, R, K, V> Iterator for AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  type Item = AllVersions<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .upper_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          current_version: version,
          entries: ent,
        });
      }
    }
  }
}

impl<'a, Q, R, K, V> DoubleEndedIterator for AllVersionsRange<'a, Q, R, K, V>
where
  K: Ord + Borrow<Q>,
  R: RangeBounds<Q>,
  Q: Ord + ?Sized,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let ent = self.range.next_back()?;
      let min = *ent.value().front().unwrap().key();
      if let Some(version) = ent
        .value()
        .lower_bound(Bound::Included(&self.version))
        .and_then(|ent| {
          if ent.value().is_some() {
            Some(*ent.key())
          } else {
            None
          }
        })
      {
        return Some(AllVersions {
          max_version: version,
          min_version: min,
          current_version: version,
          entries: ent,
        });
      }
    }
  }
}
