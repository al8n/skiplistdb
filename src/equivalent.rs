use super::*;

mod read;
pub use read::*;

mod write;
pub use write::*;

/// A reference to an entry in the write transaction.
pub struct EntryRef<'a, K, V> {
  ent: mwmr::EntryRef<'a, K, V>,
}

impl<'a, K, V> Clone for EntryRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, K, V> Copy for EntryRef<'a, K, V> {}

impl<'a, K, V> EntryRef<'a, K, V> {
  /// Create a new entry reference.
  #[inline]
  pub const fn new(ent: mwmr::EntryRef<'a, K, V>) -> Self {
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
pub struct OptionEntryRef<'a, K, V> {
  ent: mwmr::EntryRef<'a, K, V>,
}

impl<'a, K, V> Clone for OptionEntryRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, K, V> Copy for OptionEntryRef<'a, K, V> {}

impl<'a, K, V> OptionEntryRef<'a, K, V> {
  /// Create a new entry reference.
  #[inline]
  pub const fn new(ent: mwmr::EntryRef<'a, K, V>) -> Self {
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
  map: SkipMap<K, SkipMap<u64, Option<Arc<V>>>>,
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
  fn get<Q>(&self, key: &Q, version: u64) -> Option<Arc<V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.inner.map.get(key)?;
    ent
      .value()
      .upper_bound(Bound::Included(&version))
      .and_then(|vent| vent.value().clone())
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
}

/// An iterator over a all values with the same key in different versions.
pub struct AllVersions<'a, K, V> {
  max_version: u64,
  min_version: u64,
  current_version: u64,
  entries: MapEntry<'a, K, SkipMap<u64, Option<Arc<V>>>>,
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
  type Item = Option<Arc<V>>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.current_version >= self.max_version {
      return None;
    }

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

        ent.value().clone()
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
        ent.value().clone()
      })
  }
}

impl<'a, K, V> FusedIterator for AllVersions<'a, K, V> {}

/// An iterator over a all values with the same key in different versions.
pub struct AllVersionsWithPending<'a, K, V> {
  pending: Option<OptionEntryRef<'a, K, V>>,
  committed: Option<AllVersions<'a, K, V>>,
}

impl<'a, K, V> Clone for AllVersionsWithPending<'a, K, V> {
  fn clone(&self) -> Self {
    Self {
      pending: self.pending,
      committed: self.committed.clone(),
    }
  }
}

impl<'a, K, V> Iterator for AllVersionsWithPending<'a, K, V> {
  type Item = Either<OptionEntryRef<'a, K, V>, Option<Arc<V>>>;

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

impl<'a, K, V> FusedIterator for AllVersionsWithPending<'a, K, V> {}

/// Item reference yielded by the iterator.
pub struct ItemRef<'a, K, V> {
  version: u64,
  ent: MapEntry<'a, K, SkipMap<u64, Option<Arc<V>>>>,
}

impl<'a, K, V> ItemRef<'a, K, V> {
  /// Get the key of the entry.
  #[inline]
  pub fn key(&self) -> &K {
    self.ent.key()
  }

  /// Get the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Get the value of the entry.
  #[inline]
  pub fn value(&self) -> Arc<V> {
    // We already know that the value can't be None, when yielding the item.
    let ent = self.ent.value().get(&self.version).unwrap();
    // We already know that the value can't be None, when yielding the item.
    ent.value().clone().unwrap()
  }
}

/// An iterator over the entries of a `EquivalentDB`.
pub struct Iter<'a, K, V> {
  iter: crossbeam_skiplist::map::Iter<'a, K, SkipMap<u64, Option<Arc<V>>>>,
  version: u64,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
  K: Ord,
{
  type Item = ItemRef<'a, K, V>;

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
        return Some(ItemRef { version, ent });
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
        return Some(ItemRef { version, ent });
      }
    }
  }
}

impl<'a, K, V> FusedIterator for Iter<'a, K, V> where K: Ord {}


/// An iterator over the entries of a `EquivalentDB`.
pub struct AllVersionsIter<'a, K, V> {
  iter: crossbeam_skiplist::map::Iter<'a, K, SkipMap<u64, Option<Arc<V>>>>,
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

impl<'a, K, V> FusedIterator for AllVersionsIter<'a, K, V> where K: Ord {}
