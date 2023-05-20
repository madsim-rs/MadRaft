//! Some data structures utils.

use super::model::{Entry, EntryValue, Model};
use std::{collections::HashMap, ptr::NonNull};

/// a pointer type
pub(super) type MaybeNull<T> = Option<NonNull<T>>;

fn maybe_null<T>(elem: &mut T) -> MaybeNull<T> {
    unsafe { Some(NonNull::new_unchecked(elem as _)) }
}

#[derive(Debug)]
struct Sentinel<In, Out> {
    next: MaybeNull<EntryNode<In, Out>>,
}

#[derive(Debug)]
pub(super) struct EntryNode<In, Out> {
    pub(super) value: EntryValue<In, Out>,
    matched: MaybeNull<EntryNode<In, Out>>,
    pub(super) id: usize,
    prev: MaybeNull<EntryNode<In, Out>>,
    next: MaybeNull<EntryNode<In, Out>>,
}

impl<In, Out> EntryNode<In, Out> {
    pub(super) fn next_mut(&self) -> Option<&mut EntryNode<In, Out>> {
        Some(unsafe { self.next?.as_mut() })
    }

    pub(super) fn matched_mut(&self) -> Option<&mut EntryNode<In, Out>> {
        Some(unsafe { self.matched?.as_mut() })
    }

    pub(super) fn unwrap_in(&self) -> &In {
        match &self.value {
            EntryValue::Call(v) => v,
            EntryValue::Return(_) => panic!("type mismatch"),
        }
    }
    pub(super) fn unwrap_out(&self) -> &Out {
        match &self.value {
            EntryValue::Call(_) => panic!("type mismatch"),
            EntryValue::Return(v) => v,
        }
    }

    /// detach self and self's match from list
    pub(super) fn lift(&mut self) -> (Box<EntryNode<In, Out>>, Box<EntryNode<In, Out>>) {
        assert!(matches!(self.value, EntryValue::Call(_)));
        unsafe {
            self.prev.unwrap().as_mut().next = self.next;
            // since calls and returns are paired, call.next won't be None.
            self.next.unwrap().as_mut().prev = self.prev;
            let self_box = Box::from_raw(self as _);
            let matched = self.matched_mut().unwrap();
            matched.prev.unwrap().as_mut().next = matched.next;
            if let Some(mut n) = matched.next {
                n.as_mut().prev = matched.prev;
            }
            (self_box, Box::from_raw(matched as _))
        }
    }

    /// re-attach self and self's match to their original places
    pub(super) fn unlift(self: Box<Self>, matched: Box<Self>) {
        assert!(matches!(self.value, EntryValue::Call(_)));
        unsafe {
            let self_ptr = NonNull::new_unchecked(Box::into_raw(self));
            let matched_ptr = NonNull::new_unchecked(Box::into_raw(matched));
            matched_ptr.as_ref().prev.unwrap().as_mut().next = Some(matched_ptr);
            if let Some(mut n) = matched_ptr.as_ref().next {
                n.as_mut().prev = Some(matched_ptr);
            }
            self_ptr.as_ref().prev.unwrap().as_mut().next = Some(self_ptr);
            // since calls and returns are paired, call.next won't be None.
            self_ptr.as_ref().next.unwrap().as_mut().prev = Some(self_ptr);
        }
    }
}

/// A linked list.
#[derive(Debug)]
pub(super) struct LinkedEntries<In, Out> {
    sentinel: Sentinel<In, Out>,
}

impl<In, Out> LinkedEntries<In, Out> {
    pub(super) fn new() -> Self {
        Self {
            sentinel: Sentinel { next: None },
        }
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.sentinel.next.is_none()
    }

    fn push_front_node(&mut self, node: EntryNode<In, Out>) {
        assert!(node.prev.is_none());
        assert!(node.next.is_none());
        unsafe {
            let mut node_ptr = NonNull::new_unchecked(Box::into_raw(Box::new(node)));
            if let Some(mut prev_head) = self.sentinel.next {
                node_ptr.as_mut().next = Some(prev_head);
                prev_head.as_mut().prev = Some(node_ptr);
            } else {
                self.sentinel.next = Some(node_ptr);
            }
        }
    }

    pub(super) fn front_mut(&self) -> Option<&mut EntryNode<In, Out>> {
        Some(unsafe { self.sentinel.next?.as_mut() })
    }

    fn pop_front_node(&mut self) -> Option<Box<EntryNode<In, Out>>> {
        self.sentinel.next.map(|head| unsafe {
            let node = Box::from_raw(head.as_ptr());
            self.sentinel.next = node.next;
            node
        })
    }
}

impl<In, Out> Drop for LinkedEntries<In, Out> {
    fn drop(&mut self) {
        while self.pop_front_node().is_some() {}
    }
}

impl<M: Model> From<Vec<Entry<M>>> for LinkedEntries<M::In, M::Out> {
    fn from(value: Vec<Entry<M>>) -> Self {
        let mut me = LinkedEntries::new();
        // id -> return entry of this id
        let mut matches: HashMap<usize, MaybeNull<EntryNode<M::In, M::Out>>> =
            HashMap::with_capacity((value.len() + 1) / 2);
        for entry in value.into_iter().rev() {
            let node = match entry.value {
                EntryValue::Call(v) => EntryNode {
                    value: EntryValue::Call(v),
                    matched: matches[&entry.id], // call -> return
                    id: entry.id,
                    prev: None,
                    next: None,
                },
                EntryValue::Return(v) => {
                    let mut node = EntryNode {
                        value: EntryValue::Return(v),
                        matched: None,
                        id: entry.id,
                        prev: None,
                        next: None,
                    };
                    matches.insert(entry.id, maybe_null(&mut node));
                    node
                }
            };
            me.push_front_node(node);
        }
        me
    }
}
