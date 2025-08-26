#![allow(unused)]

#![warn(
    clippy::pedantic,
    missing_debug_implementations,
    missing_docs,
    noop_method_call,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_op_in_unsafe_fn,
    unused_lifetimes,
    unused_qualifications
)]
#![allow(
    clippy::items_after_statements,

    // Repetition is used in `Send`+`Sync` bounds so each one can be individually commented.
    clippy::type_repetition_in_bounds,

    // This is a silly lint; I always turbofish the transmute so it's fine
    clippy::transmute_ptr_to_ptr,

    // Has false positives: https://github.com/rust-lang/rust-clippy/issues/7812
    clippy::redundant_closure,

    // I export all the types at the super root, so this lint is pointless.
    clippy::module_name_repetitions,
)]

pub mod id;
pub use id::Id;

mod list;
pub use list::{Cursor, CursorMut, PinList, Types};

mod node;
pub use node::{InitializedNode, Node, NodeData};

mod util;

#[cfg(test)]
mod tests {
    use tokio::pin;

    use super::id;
    use core::pin::Pin;
    use std::boxed::Box;
    use std::panic;
    use std::process::abort;
    use std::ptr;

    type PinListTypes = dyn super::Types<
        Id = id::Checked,
        // Use boxes because they better detect double-frees, type mismatches and other errors.
        Protected = Box<u8>,
        Released = Box<u16>,
        Acquired = (),
        Unprotected = Box<u32>,
    >;
    type PinList = super::PinList<PinListTypes>;

    #[test]
    fn empty_lists() {
        let mut list = PinList::new(id::Checked::new());
        let list_ptr: *const _ = &raw const list;

        let assert_ghost_cursor = |cursor: super::Cursor<'_, _>| {
            assert!(ptr::eq(cursor.list(), list_ptr));
            assert_eq!(cursor.protected(), None);
            assert_eq!(cursor.unprotected(), None);
        };
        let assert_ghost_cursor_mut = |mut cursor: super::CursorMut<'_, _>| {
            assert!(ptr::eq(cursor.list(), list_ptr));
            assert_eq!(cursor.protected(), None);
            assert_eq!(cursor.protected_mut(), None);
            assert_eq!(cursor.unprotected(), None);
            assert_eq!(cursor.release_current(Box::new(5)), Err(Box::new(5)));
            assert!(!cursor.release_current_with(|_| abort()));
            assert!(!cursor.release_current_with_or(|_| abort(), || abort()));
            assert_ghost_cursor(cursor.as_shared());
        };

        assert!(list.is_empty());

        assert_ghost_cursor(list.cursor_ghost());
        assert_ghost_cursor(list.cursor_front());
        assert_ghost_cursor(list.cursor_back());
        assert_ghost_cursor_mut(list.cursor_ghost_mut());
        assert_ghost_cursor_mut(list.cursor_front_mut());
        assert_ghost_cursor_mut(list.cursor_back_mut());
    }

    #[test]
    fn single_node_with_unlink() {
        let mut list = PinList::new(id::Checked::new());

        let node = super::Node::new();
        pin!(node);
        assert!(node.is_initial());
        assert!(node.initialized().is_none());
        assert!(node.as_mut().initialized_mut().is_none());

        let mut protected = Box::new(0);
        let unprotected = Box::new(1);
        list.push_front(node.as_mut(), protected.clone(), unprotected.clone());

        assert!(!node.is_initial());

        let initialized = node.initialized().unwrap();
        assert_eq!(initialized.unprotected(), &unprotected);
        assert_eq!(initialized.protected(&list), Some(&protected));
        assert_eq!(initialized.protected_mut(&mut list), Some(&mut protected));

        let initialized = node.as_mut().initialized_mut().unwrap();
        assert_eq!(initialized.unprotected(), &unprotected);
        assert_eq!(initialized.protected(&list), Some(&protected));
        assert_eq!(initialized.protected_mut(&mut list), Some(&mut protected));

        assert!(!list.is_empty());

        let check_cursor = |mut cursor: super::Cursor<'_, _>| {
            assert_eq!(cursor.protected().unwrap(), &protected);
            assert_eq!(cursor.unprotected().unwrap(), &unprotected);
            cursor.move_next();
            assert_eq!(cursor.protected(), None);
            for _ in 0..10 {
                cursor.move_previous();
                assert_eq!(cursor.protected().unwrap(), &protected);
                cursor.move_previous();
                assert_eq!(cursor.protected(), None);
            }
        };
        let check_cursor_mut = |mut cursor: super::CursorMut<'_, _>| {
            assert_eq!(cursor.protected().unwrap(), &protected);
            assert_eq!(cursor.protected_mut().unwrap(), &protected);
            assert_eq!(cursor.unprotected().unwrap(), &unprotected);
            for _ in 0..7 {
                cursor.move_next();
                assert_eq!(cursor.protected(), None);
                cursor.move_next();
                assert_eq!(cursor.protected().unwrap(), &protected);
            }
            for _ in 0..10 {
                cursor.move_previous();
                assert_eq!(cursor.protected(), None);
                cursor.move_previous();
                assert_eq!(cursor.protected().unwrap(), &protected);
            }
            check_cursor(cursor.as_shared());
            check_cursor(cursor.into_shared());
        };

        check_cursor(list.cursor_front());
        check_cursor(list.cursor_back());
        check_cursor_mut(list.cursor_front_mut());
        check_cursor_mut(list.cursor_back_mut());

        assert_eq!(
            initialized.unlink(&mut list).unwrap(),
            (protected, unprotected)
        );

        assert!(node.is_initial());
        assert!(node.initialized().is_none());
        assert!(node.as_mut().initialized_mut().is_none());

        assert!(list.is_empty());
    }

    #[test]
    fn removal() {
        let mut list = PinList::new(id::Checked::new());

        let node = super::Node::new();
        pin!(node);

        let protected = Box::new(0);
        let removed = Box::new(1);
        let unprotected = Box::new(2);

        let initialized_node =
            list.push_back(node.as_mut(), protected.clone(), unprotected.clone());

        assert_eq!(
            list.cursor_front_mut()
                .release_current(removed.clone())
                .unwrap(),
            protected
        );

        assert_eq!(
            initialized_node.take_removed(&list).unwrap(),
            (removed, unprotected)
        );
        assert!(node.is_initial());
        assert!(list.is_empty());
    }

    #[test]
    fn removal_fallback() {
        let mut list = PinList::new(id::Checked::new());

        let node = super::Node::new();
        pin!(node);

        let protected = Box::new(0);
        let removed = Box::new(1);
        let unprotected = Box::new(2);

        let initialized_node =
            list.push_front(node.as_mut(), protected.clone(), unprotected.clone());

        let mut cursor = list.cursor_front_mut();
        let res = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            cursor.release_current_with_or(
                |p| {
                    assert_eq!(p, protected);
                    panic::panic_any(491_u32)
                },
                || removed.clone(),
            )
        }));
        assert_eq!(cursor.protected(), None);
        assert_eq!(res.unwrap_err().downcast::<u32>().unwrap(), Box::new(491));

        assert_eq!(
            initialized_node.take_removed(&list).unwrap(),
            (removed, unprotected),
        );
        assert!(node.is_initial());
        assert!(list.is_empty());
    }

    #[test]
    fn multinode() {
        let mut list = PinList::new(id::Checked::new());

        let mut nodes = (0..7)
            .map(|_| Box::pin(super::Node::new()))
            .collect::<Box<[_]>>();

        fn assert_order<const N: usize>(list: &mut PinList, order: [u8; N]) {
            // Forwards iteration
            let mut cursor = list.cursor_ghost_mut();
            for number in order {
                cursor.move_next();
                assert_eq!(**cursor.protected().unwrap(), number);
                assert_eq!(**cursor.protected_mut().unwrap(), number);
                assert_eq!(**cursor.unprotected().unwrap(), u32::from(number));
            }
            cursor.move_next();
            assert_eq!(cursor.protected(), None);
            assert_eq!(cursor.protected_mut(), None);
            assert_eq!(cursor.unprotected(), None);

            // Reverse iteration
            for number in order.into_iter().rev() {
                cursor.move_previous();
                assert_eq!(**cursor.protected().unwrap(), number);
                assert_eq!(**cursor.protected_mut().unwrap(), number);
                assert_eq!(**cursor.unprotected().unwrap(), u32::from(number));
            }
            cursor.move_previous();
            assert_eq!(cursor.protected(), None);
            assert_eq!(cursor.protected_mut(), None);
            assert_eq!(cursor.unprotected(), None);
        }

        fn cursor(list: &mut PinList, index: usize) -> super::CursorMut<'_, PinListTypes> {
            let mut cursor = list.cursor_front_mut();
            for _ in 0..index {
                cursor.move_next();
                cursor.protected().unwrap();
            }
            cursor
        }

        // ghost before; ghost after
        list.cursor_ghost_mut()
            .insert_before(nodes[0].as_mut(), Box::new(0), Box::new(0));
        assert_order(&mut list, [0]);

        // ghost before; node after; insert_after
        list.cursor_ghost_mut()
            .insert_after(nodes[1].as_mut(), Box::new(1), Box::new(1));
        assert_order(&mut list, [1, 0]);

        // ghost before; node after; insert_before
        cursor(&mut list, 0).insert_before(nodes[2].as_mut(), Box::new(2), Box::new(2));
        assert_order(&mut list, [2, 1, 0]);

        // node before; ghost after; insert_after
        cursor(&mut list, 2).insert_after(nodes[3].as_mut(), Box::new(3), Box::new(3));
        assert_order(&mut list, [2, 1, 0, 3]);

        // node before; ghost after; insert_before
        list.cursor_ghost_mut()
            .insert_before(nodes[4].as_mut(), Box::new(4), Box::new(4));
        assert_order(&mut list, [2, 1, 0, 3, 4]);

        // node before; node after; insert_after
        cursor(&mut list, 0).insert_after(nodes[5].as_mut(), Box::new(5), Box::new(5));
        assert_order(&mut list, [2, 5, 1, 0, 3, 4]);

        // node before; node after; insert_before
        cursor(&mut list, 1).insert_before(nodes[6].as_mut(), Box::new(6), Box::new(6));
        assert_order(&mut list, [2, 6, 5, 1, 0, 3, 4]);

        fn unlink(
            list: &mut PinList,
            nodes: &mut [Pin<Box<super::Node<PinListTypes>>>],
            index: u8,
        ) {
            let node = nodes[usize::from(index)].as_mut();
            let node = node.initialized_mut().expect("already unlinked");
            let (protected, unprotected) = node.unlink(list).unwrap();
            assert_eq!(*protected, index);
            assert_eq!(*unprotected, u32::from(index));
        }

        fn remove(
            list: &mut PinList,
            nodes: &mut [Pin<Box<super::Node<PinListTypes>>>],
            index: u8,
        ) {
            let node = nodes[usize::from(index)].as_mut();
            let node = node.initialized_mut().expect("already unlinked");
            let mut cursor = node.cursor_mut(&mut *list).unwrap();
            let removed = Box::new(u16::from(index));
            assert_eq!(*cursor.release_current(removed).unwrap(), index);
            let (removed, unprotected) = node.take_removed(&*list).unwrap();
            assert_eq!(*removed, u16::from(index));
            assert_eq!(*unprotected, u32::from(index));
        }

        // node before; node after; unlink
        unlink(&mut list, &mut nodes, 6);
        assert_order(&mut list, [2, 5, 1, 0, 3, 4]);

        // node before; node after; remove
        remove(&mut list, &mut nodes, 5);
        assert_order(&mut list, [2, 1, 0, 3, 4]);

        // node before; ghost after; unlink
        unlink(&mut list, &mut nodes, 4);
        assert_order(&mut list, [2, 1, 0, 3]);

        // node before; ghost after; remove
        remove(&mut list, &mut nodes, 3);
        assert_order(&mut list, [2, 1, 0]);

        // ghost before; node after; unlink
        unlink(&mut list, &mut nodes, 2);
        assert_order(&mut list, [1, 0]);

        // ghost before; node after; remove
        remove(&mut list, &mut nodes, 1);
        assert_order(&mut list, [0]);

        // ghost before; ghost after; unlink
        unlink(&mut list, &mut nodes, 0);
        assert_order(&mut list, []);
    }
}
