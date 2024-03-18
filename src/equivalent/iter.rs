use super::*;

/// An iterator over a all values with the same key in different versions.
pub struct WriteTransactionAllVersions<'a, K, V> {
  pub(crate) pending: Option<OptionPendingRef<'a, K, V>>,
  pub(crate) committed: Option<AllVersions<'a, K, V>>,
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

  pub(crate) fn new(
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

impl<'a, K, V, S> DoubleEndedIterator for WriteTransactionIter<'a, K, V, S>
where
  K: Ord + core::hash::Hash + Eq + 'static,
  S: BuildHasher + 'static,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      // Get the last items from both iterators without removing them
      let pending_back = self.pendings.next_back();
      let committed_back = self.committed.next_back();
      if let Some(item) = &committed_back {
        if let Some(m) = &mut self.marker {
          m.mark(item.key());
        }
      }

      match (pending_back, committed_back) {
        (Some((pending_key, pending_value)), Some(committed)) => {
          // Compare the keys to determine which to yield
          match pending_key.cmp(committed.key()) {
            cmp::Ordering::Greater => {
              // Pending has the greater key, so yield this
              self.last_yielded_key = Some(Either::Left(pending_key));
              match &pending_value.value {
                Some(value) => return Some(Either::Left((pending_key, value))),
                None => continue,
              }
            }
            cmp::Ordering::Equal => {
              // Keys are equal, prefer pending and skip committed
              if let Some(item) = self.committed.next_back() {
                if let Some(m) = &mut self.marker {
                  m.mark(item.key());
                }
              }
              continue;
            }
            cmp::Ordering::Less => {
              // Committed has the greater key
              if self.last_yielded_key.as_ref().map_or(true, |k| match k {
                Either::Left(k) => *k != committed.key(),
                Either::Right(item) => item.key() != committed.key(),
              }) {
                self.last_yielded_key = Some(Either::Right(committed.clone()));
                return Some(Either::Right(committed));
              } else {
                // If the key has already been yielded, skip it
                if let Some(item) = self.committed.next_back() {
                  if let Some(m) = &mut self.marker {
                    m.mark(item.key());
                  }
                }
                continue;
              }
            }
          }
        }
        (Some((pending_key, pending_value)), None) => {
          // Only pending items are left
          self.last_yielded_key = Some(Either::Left(pending_key));
          match &pending_value.value {
            Some(value) => return Some(Either::Left((pending_key, value))),
            None => continue,
          }
        }
        (None, Some(committed)) => {
          // Only committed items are left
          if self.last_yielded_key.as_ref().map_or(true, |k| match k {
            Either::Left(k) => *k != committed.key(),
            Either::Right(item) => item.key() != committed.key(),
          }) {
            self.last_yielded_key = Some(Either::Right(committed.clone()));
            return Some(Either::Right(committed));
          } else {
            if let Some(item) = self.committed.next_back() {
              if let Some(m) = &mut self.marker {
                m.mark(item.key());
              }
            }
            continue;
          }
        }
        (None, None) => return None, // Both iterators are exhausted
      }
    }
  }
}

/// Iterator over the entries of the write transaction.
pub struct WriteTransactionAllVersionsIter<'a, K, V, S> {
  db: &'a EquivalentDB<K, V, S>,
  pendings: BTreeMapIter<'a, K, EntryValue<V>>,
  committed: AllVersionsIter<'a, K, V>,
  next_pending: Option<(&'a K, &'a EntryValue<V>)>,
  next_committed: Option<AllVersions<'a, K, V>>,
  last_yielded_key: Option<Either<&'a K, AllVersions<'a, K, V>>>,
  marker: Option<Marker<'a, HashCm<K, S>>>,
  version: u64,
}

impl<'a, K, V, S> WriteTransactionAllVersionsIter<'a, K, V, S>
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

  pub(crate) fn new(
    db: &'a EquivalentDB<K, V, S>,
    version: u64,
    pendings: BTreeMapIter<'a, K, EntryValue<V>>,
    committed: AllVersionsIter<'a, K, V>,
    marker: Option<Marker<'a, HashCm<K, S>>>,
  ) -> Self {
    let mut iterator = WriteTransactionAllVersionsIter {
      db,
      pendings,
      committed,
      next_pending: None,
      next_committed: None,
      last_yielded_key: None,
      marker,
      version,
    };

    iterator.advance_pending();
    iterator.advance_committed();

    iterator
  }
}

impl<'a, K, V, S> Iterator for WriteTransactionAllVersionsIter<'a, K, V, S>
where
  K: Ord + core::hash::Hash + Eq + 'static,
  S: BuildHasher + 'static,
{
  type Item = WriteTransactionAllVersions<'a, K, V>;

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
              return Some(WriteTransactionAllVersions {
                pending: Some(OptionPendingRef::new(mwmr::EntryRef {
                  data: match value.value {
                    Some(ref value) => mwmr::EntryDataRef::Insert { key, value },
                    None => mwmr::EntryDataRef::Remove(key),
                  },
                  version: value.version,
                })),
                committed: self.db.inner.map.get_all_versions(key, self.version),
              });
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
                return Some(WriteTransactionAllVersions {
                  pending: None,
                  committed: Some(committed),
                });
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
            Some(val) => {
              return Some(WriteTransactionAllVersions {
                pending: Some(OptionPendingRef::new(mwmr::EntryRef {
                  data: mwmr::EntryDataRef::Insert { key, value: val },
                  version: value.version,
                })),
                committed: self.db.inner.map.get_all_versions(key, self.version),
              })
            }
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
            return Some(WriteTransactionAllVersions {
              pending: None,
              committed: Some(committed),
            });
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
