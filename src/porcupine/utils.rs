//! Some data structures utils.

use super::model::{Entry, EntryValue, Model};
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

/// Pointer type used in the linked list.
#[derive(Debug)]
pub(super) struct Ptr<T>(NonNull<T>);

impl<T> Ptr<T> {
    fn new(item: Box<T>) -> Self {
        // SAFETY: pointer is get from a `Box`.
        unsafe { Self(NonNull::new_unchecked(Box::into_raw(item))) }
    }

    /// Reclaim ownership from the linked list.
    ///
    /// # Safety
    ///
    /// The pointer must not be accessed later.
    unsafe fn reclaim(self) -> Box<T> {
        Box::from_raw(self.0.as_ptr())
    }
}

impl<T> Copy for Ptr<T> {}

impl<T> Clone for Ptr<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Deref for Ptr<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: `Ptr` initialized with a valid pointer.
        unsafe { self.0.as_ref() }
    }
}

impl<T> DerefMut for Ptr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: `Ptr` initialized with a valid pointer.
        unsafe { self.0.as_mut() }
    }
}

#[derive(Debug)]
pub(super) struct EntryNode<In, Out> {
    pub(super) value: EntryValue<In, Out>,
    matched: Option<Ptr<EntryNode<In, Out>>>,
    pub(super) id: usize,
    prev: Option<Ptr<EntryNode<In, Out>>>,
    next: Option<Ptr<EntryNode<In, Out>>>,
}

impl<In, Out> EntryNode<In, Out> {
    pub(super) fn get_ref<'a, 'b>(self: &'a Box<Self>) -> &'b Self {
        // SAFETY: self will not be dropped
        unsafe { &*(self.as_ref() as *const Self) }
    }
    pub(super) fn next(&self) -> Option<&EntryNode<In, Out>> {
        self.next.as_deref()
    }

    pub(super) fn matched(&self) -> Option<&EntryNode<In, Out>> {
        self.matched.as_deref()
    }

    pub(super) fn unwrap_in(&self) -> &In {
        match &self.value {
            EntryValue::Call(v) => v,
            _ => panic!("type mismatch"),
        }
    }

    pub(super) fn unwrap_out(&self) -> &Out {
        match &self.value {
            EntryValue::Return(v) => v,
            _ => panic!("type mismatch"),
        }
    }

    /// detach self and self's match from list
    pub(super) fn lift(&self) -> (Box<EntryNode<In, Out>>, Box<EntryNode<In, Out>>) {
        assert!(matches!(self.value, EntryValue::Call(_)));
        self.prev.unwrap().next = self.next;
        // since calls and returns are paired, call.next won't be None.
        self.next.unwrap().prev = self.prev;
        // SAFETY: `Box` is used to transfer ownership, the `EntryNode` in it is inaccessible
        // from the linked list anymore.
        let self_box = unsafe { Box::from_raw(self as *const _ as *mut _) };
        let matched = self.matched().unwrap();
        matched.prev.unwrap().next = matched.next;
        if let Some(mut n) = matched.next {
            n.prev = matched.prev;
        }
        // SAFETY: `Box` is used to transfer ownership, the `EntryNode` in it is inaccessible
        // from the linked list anymore.
        let match_box = unsafe { Box::from_raw(matched as *const _ as *mut _) };
        (self_box, match_box)
    }

    /// re-attach self and self's match to their original places
    pub(super) fn unlift(self: Box<Self>, matched: Box<Self>) {
        assert!(matches!(self.value, EntryValue::Call(_)));
        let self_ptr = Ptr::new(self);
        let matched_ptr = Ptr::new(matched);
        matched_ptr.prev.unwrap().next = Some(matched_ptr);
        if let Some(mut n) = matched_ptr.next {
            n.prev = Some(matched_ptr);
        }
        self_ptr.prev.unwrap().next = Some(self_ptr);
        // since calls and returns are paired, call.next won't be None.
        self_ptr.next.unwrap().prev = Some(self_ptr);
    }
}

/// A linked list.
#[derive(Debug)]
pub(super) struct LinkedEntries<In, Out> {
    sentinel: Ptr<EntryNode<In, Out>>,
}

impl<In, Out> LinkedEntries<In, Out> {
    pub(super) fn new() -> Self {
        Self {
            sentinel: Ptr::new(Box::new(EntryNode {
                value: EntryValue::Null,
                matched: None,
                id: usize::MAX,
                prev: None,
                next: None,
            })),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sentinel.next.is_none()
    }

    pub(super) fn front(&self) -> Option<&EntryNode<In, Out>> {
        self.sentinel.next.as_deref()
    }

    fn push_front_node(
        &mut self,
        node: EntryNode<In, Out>,
        matches: &mut HashMap<usize, Ptr<EntryNode<In, Out>>>,
    ) {
        assert!(node.prev.is_none());
        assert!(node.next.is_none());
        let mut node_ptr = Ptr::new(Box::new(node));
        if matches!(node_ptr.value, EntryValue::Return(_)) {
            matches.insert(node_ptr.id, node_ptr.clone());
        }
        let prev_head_ptr = self.sentinel.next;
        node_ptr.prev = Some(self.sentinel);
        node_ptr.next = prev_head_ptr;
        self.sentinel.next = Some(node_ptr);
        prev_head_ptr.map(|mut head| head.prev = Some(node_ptr));
    }

    fn pop_front_node(&mut self) -> Option<Box<EntryNode<In, Out>>> {
        self.sentinel.next.map(|head| {
            // SAFETY: used in drop
            let node = unsafe { head.reclaim() };
            self.sentinel.next = node.next;
            node
        })
    }
}

impl<In, Out> Drop for LinkedEntries<In, Out> {
    fn drop(&mut self) {
        while self.pop_front_node().is_some() {}
        // SAFETY: used in drop
        unsafe {
            _ = self.sentinel.reclaim();
        }
    }
}

impl<M: Model> From<Vec<Entry<M>>> for LinkedEntries<M::In, M::Out> {
    fn from(value: Vec<Entry<M>>) -> Self {
        let mut me = LinkedEntries::new();
        // id -> return entry of this id
        let mut matches: HashMap<usize, Ptr<EntryNode<M::In, M::Out>>> =
            HashMap::with_capacity((value.len() + 1) / 2);
        for entry in value.into_iter().rev() {
            let node = match entry.value {
                EntryValue::Call(v) => EntryNode {
                    value: EntryValue::Call(v),
                    matched: Some(matches[&entry.id]), // call -> return
                    id: entry.id,
                    prev: None,
                    next: None,
                },
                EntryValue::Return(v) => EntryNode {
                    value: EntryValue::Return(v),
                    matched: None,
                    id: entry.id,
                    prev: None,
                    next: None,
                },
                _ => unreachable!("EntryValue::Null is only used in senital"),
            };
            me.push_front_node(node, &mut matches);
        }
        me
    }
}
