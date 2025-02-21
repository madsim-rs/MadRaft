//! Some data structures utils.

use super::model::{Entry, EntryValue, Model};
use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
};

type SharedPtr<T> = Option<Rc<RefCell<T>>>;
type WeakPtr<T> = Option<Weak<RefCell<T>>>;

#[derive(Debug)]
pub(super) struct EntryView<In, Out> {
    value: Rc<EntryValue<In, Out>>,
    matched: SharedPtr<EntryNode<In, Out>>,
    pub(super) id: usize,
    prev: WeakPtr<EntryNode<In, Out>>,
    next: WeakPtr<EntryNode<In, Out>>,
}

#[derive(Debug)]
pub(super) struct EntryNode<In, Out> {
    pub(super) value: Rc<EntryValue<In, Out>>,
    matched: WeakPtr<EntryNode<In, Out>>,
    pub(super) id: usize,
    prev: WeakPtr<EntryNode<In, Out>>,
    next: SharedPtr<EntryNode<In, Out>>,
}

impl<In, Out> EntryView<In, Out> {
    /// re-attach self and self's match to their original places
    pub(super) fn unlift<'a>(self: Self) -> Rc<RefCell<EntryNode<In, Out>>> {
        assert!(matches!(*self.value, EntryValue::Call(_)));
        let matched_ptr = self.matched.clone().unwrap();
        let self_ptr = Rc::new(RefCell::new(EntryNode {
            value: self.value.clone(),
            matched: Some(Rc::downgrade(&matched_ptr)),
            id: self.id,
            prev: self.prev.clone(),
            next: Some(self.next.unwrap().upgrade().unwrap()),
        }));
        matched_ptr
            .as_ref()
            .borrow()
            .prev
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap()
            .borrow_mut()
            .next = Some(matched_ptr.clone());
        if let Some(n) = matched_ptr.as_ref().borrow().next.as_ref() {
            n.borrow_mut().prev = Some(Rc::downgrade(&matched_ptr));
        }
        self_ptr
            .as_ref()
            .borrow()
            .prev
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap()
            .borrow_mut()
            .next = Some(self_ptr.clone());
        // since calls and returns are paired, call.next won't be None.
        self_ptr
            .as_ref()
            .borrow()
            .next
            .as_ref()
            .unwrap()
            .borrow_mut()
            .prev = Some(Rc::downgrade(&self_ptr));
        self_ptr
    }
}

impl<In, Out> EntryNode<In, Out> {
    pub(super) fn next(this: Rc<RefCell<Self>>) -> SharedPtr<EntryNode<In, Out>> {
        this.as_ref().borrow().next.clone()
    }

    pub(super) fn matched(&self) -> SharedPtr<EntryNode<In, Out>> {
        Some(self.matched.as_ref()?.clone().upgrade().unwrap())
    }

    pub(super) fn unwrap_in(&self) -> &In {
        match *self.value {
            EntryValue::Call(ref v) => v,
            _ => panic!("type mismatch"),
        }
    }

    pub(super) fn unwrap_out(&self) -> &Out {
        match *self.value {
            EntryValue::Return(ref v) => v,
            _ => panic!("type mismatch"),
        }
    }

    /// detach self and self's match from list
    pub(super) fn lift(&self) -> EntryView<In, Out> {
        assert!(matches!(*self.value, EntryValue::Call(_)));
        self.prev
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap()
            .borrow_mut()
            .next = self.next.clone();
        // since calls and returns are paired, call.next won't be None.
        self.next.as_ref().unwrap().borrow_mut().prev = self.prev.clone();
        let self_view = EntryView {
            value: self.value.clone(),
            matched: self.matched(),
            id: self.id,
            prev: self.prev.clone(),
            next: Some(Rc::downgrade(self.next.as_ref().unwrap())),
        };
        let matched = self.matched().unwrap();
        matched
            .as_ref()
            .borrow()
            .prev
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap()
            .borrow_mut()
            .next = matched.borrow().next.clone();
        if let Some(n) = matched.as_ref().borrow().next.as_ref() {
            n.borrow_mut().prev = matched.borrow().prev.clone();
        }
        self_view
    }
}

/// A linked list.
#[derive(Debug)]
pub(super) struct LinkedEntries<In, Out> {
    sentinel: Rc<RefCell<EntryNode<In, Out>>>,
}

impl<In, Out> LinkedEntries<In, Out> {
    pub(super) fn new() -> Self {
        Self {
            sentinel: Rc::new(RefCell::new(EntryNode {
                value: Rc::new(EntryValue::Null),
                matched: None,
                id: usize::MAX,
                prev: None,
                next: None,
            })),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sentinel.borrow().next.is_some()
    }

    pub(super) fn front(&self) -> SharedPtr<EntryNode<In, Out>> {
        self.sentinel.borrow().next.clone()
    }

    fn push_front_node(
        &mut self,
        node: EntryNode<In, Out>,
        matches: &mut HashMap<usize, Weak<RefCell<EntryNode<In, Out>>>>,
    ) {
        assert!(node.prev.is_none());
        assert!(node.next.is_none());
        let node_ptr = Rc::new(RefCell::new(node));
        if matches!(*node_ptr.borrow().value, EntryValue::Return(_)) {
            matches.insert(node_ptr.borrow().id, Rc::downgrade(&node_ptr));
        }
        let prev_head_ptr = self.sentinel.borrow().next.clone();
        node_ptr.borrow_mut().prev = Some(Rc::downgrade(&self.sentinel));
        node_ptr.borrow_mut().next = prev_head_ptr.clone();
        self.sentinel.borrow_mut().next = Some(node_ptr.clone());
        prev_head_ptr.map(|head| head.borrow_mut().prev = Some(Rc::downgrade(&node_ptr)));
    }
}

impl<M: Model> From<Vec<Entry<M>>> for LinkedEntries<M::In, M::Out> {
    fn from(value: Vec<Entry<M>>) -> Self {
        let mut me = LinkedEntries::new();
        // id -> return entry of this id
        let mut matches: HashMap<usize, Weak<RefCell<EntryNode<M::In, M::Out>>>> =
            HashMap::with_capacity((value.len() + 1) / 2);
        for entry in value.into_iter().rev() {
            let node = match entry.value {
                EntryValue::Call(v) => EntryNode {
                    value: Rc::new(EntryValue::Call(v)),
                    matched: Some(matches[&entry.id].clone()), // call -> return
                    id: entry.id,
                    prev: None,
                    next: None,
                },
                EntryValue::Return(v) => EntryNode {
                    value: Rc::new(EntryValue::Return(v)),
                    matched: None,
                    id: entry.id,
                    prev: None,
                    next: None,
                },
                _ => unreachable!("EntryValue::Null is only used in sentinel"),
            };
            me.push_front_node(node, &mut matches);
        }
        me
    }
}
