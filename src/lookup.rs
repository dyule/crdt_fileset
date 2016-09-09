use std::collections::hash_map::{HashMap};
use std::ffi::{OsString, OsStr};

use super::FileID;

pub struct IDLookup {
    head: LookupNode
}

struct LookupNode {
    id: Option<FileID>,
    children: HashMap<OsString, LookupNode>
}

impl IDLookup {
    #[inline]
    pub fn new() -> IDLookup {
        IDLookup {
            head: LookupNode::new()
        }
    }

    pub fn add_file<'a, I: 'a + IntoIterator<Item=&'a OsStr>>(&mut self, path: I, id: FileID, site_id: u32) -> String {
        let result = IDLookup::add_file_component(&mut path.into_iter(), id, &mut self.head, site_id);
        println!("{:?}", result);
        result.1.unwrap()
    }

    fn add_file_component<'a, I: 'a + Iterator<Item=&'a OsStr>>(path: &mut I, id: FileID, node: &mut LookupNode, site_id: u32) -> (bool, Option<String>) {
        if let Some(component) = path.next() {
            let mut filename = component.to_os_string().into_string().unwrap();
            let (mut try_again, mut result) = IDLookup::add_file_component(path, id, node.children.entry(component.to_os_string()).or_insert_with(LookupNode::new), site_id);
            while try_again {
                filename.push_str(&format!("(site {})", site_id));
                let lookup_result = IDLookup::add_file_component(&mut Some(OsStr::new(&filename.clone())).into_iter(), id, node.children.entry(OsString::from(filename.clone())).or_insert_with(LookupNode::new), site_id);
                try_again = lookup_result.0;
                result = lookup_result.1;
            }
            match result {
                Some(result) => {
                    (false, Some(result))
                } None => {
                    (false, Some(filename))
                }
            }

        } else {
            if node.id.is_none() {
                node.id = Some(id);
                (false, None)
            } else {
                (true, None)
            }
        }
    }

    pub fn get_id_for<'a, I: 'a +IntoIterator<Item=&'a OsStr>>(&self, path: I) -> Option<FileID> {
        IDLookup::id_lookup(path.into_iter(), &self.head)
    }

    fn id_lookup<'a, I: 'a +Iterator<Item=&'a OsStr>>(mut path: I, node: &LookupNode) -> Option<FileID> {
        if let Some(component) = path.next() {
            if let Some(child) = node.children.get(component) {
                IDLookup::id_lookup(path, child)
            } else {
                None
            }
        } else {
            node.id
        }
    }

    pub fn remove_file<'a, I: 'a +IntoIterator<Item=&'a OsStr>>(&mut self, path: I) -> Option<FileID> {
        IDLookup::remove_file_component(path.into_iter(), &mut self.head).1
    }

    fn remove_file_component<'a, I: 'a +Iterator<Item=&'a OsStr>>(mut path: I, node: &mut LookupNode) -> (bool, Option<FileID>) {
        if let Some(component) = path.next() {
            let (should_remove, result) = if let Some(child) = node.children.get_mut(component) {
                IDLookup::remove_file_component(path, child)
            } else {
                return (false, None);
            };
            if should_remove {
                node.children.remove(component);
            }
            (node.children.is_empty(), result)
        } else {
            if node.id.is_none() {
                (false, None)
            } else {
                let result = node.id;
                node.id = None;
                (node.children.is_empty(), result)
            }
        }
    }

    pub fn remove_folder<'a, I: 'a +IntoIterator<Item=&'a OsStr>>(&mut self, path: I) -> Vec<FileID>  {
        IDLookup::remove_folder_component(path.into_iter(), &mut self.head).1
    }


    fn remove_folder_component<'a, I: 'a +Iterator<Item=&'a OsStr>>(mut path: I, node: &mut LookupNode) -> (bool, Vec<FileID>) {
        if let Some(component) = path.next() {
            let (should_remove, result) = if let Some(child) = node.children.get_mut(component) {
                IDLookup::remove_folder_component(path, child)
            } else {
                return (false, Vec::new());
            };
            if should_remove {
                node.children.remove(component);
            }
            (node.children.is_empty(), result)
        } else {
            let mut removed_ids = Vec::new();
            IDLookup::collect_ids(node, &mut removed_ids);
            return (true, removed_ids);
        }
    }


    fn collect_ids(node: &LookupNode, removed_ids: &mut Vec<FileID>) {
        if let Some(id) = node.id {
            removed_ids.push(id)
        }
        for child in node.children.values() {
            IDLookup::collect_ids(child, removed_ids);
        }
    }




}

impl LookupNode {
    #[inline]
    pub fn new() -> LookupNode {
        LookupNode {
            id: None,
            children: HashMap::new()
        }
    }
}

#[cfg(test)]
mod test {
    use super::IDLookup;
    use std::ffi::{OsStr};


macro_rules! vec_str {
    ($($i: expr),*) => (vec![$(OsStr::new($i),)*]);
}
    #[test]
    fn insert_nodes() {
        let mut lookup = IDLookup::new();

        lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 13), 1);
        lookup.add_file(vec_str!["folder1", "subfolder1", "file2"], (1, 12), 1);
        lookup.add_file(vec_str!["folder1", "subfolder2", "file3"], (1, 11), 1);
        lookup.add_file(vec_str!["folder2", "subfolder1", "file4"], (1, 10), 1);
        lookup.add_file(vec_str!["folder2", "file5"], (1, 9), 1);
        lookup.add_file(vec_str!["file6"], (1, 8), 1);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file1"]), Some((1, 13)));
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file2"]), Some((1, 12)));
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder2", "file3"]), Some((1, 11)));
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "subfolder1", "file4"]), Some((1, 10)));
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "file5"]), Some((1, 9)));
        assert_eq!(lookup.get_id_for(vec_str![ "file6"]), Some((1, 8)));
        assert_eq!(lookup.get_id_for(vec_str![ "file1"]), None);
        assert_eq!(lookup.get_id_for(vec_str![ "file5"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "subfolder1", "file5"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "subfolder1", "subsubfolder1", "file5"]), None);
        assert_eq!(lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 14), 1), "file1(site 1)".to_string());
        assert_eq!(lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 15), 1), "file1(site 1)(site 1)".to_string());
        assert_eq!(lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (2, 16), 2), "file1(site 2)".to_string());
    }

    #[test]
    fn remove_nodes() {
        let mut lookup = IDLookup::new();
        lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 13), 1);
        lookup.add_file(vec_str!["folder1", "subfolder1", "file2"], (1, 12), 1);
        lookup.add_file(vec_str!["folder1", "subfolder2", "file3"], (1, 11), 1);
        lookup.add_file(vec_str!["folder2", "subfolder1", "file4"], (1, 10), 1);
        lookup.add_file(vec_str!["folder2", "file5"], (1, 9), 1);
        lookup.add_file(vec_str!["file6"], (1, 8), 1);
        assert_eq!(lookup.remove_file(vec_str!["folder1", "subfolder1", "file1"]), Some((1, 13)));
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file2"]), Some((1, 12)));
        assert_eq!(lookup.remove_file(vec_str![ "file6"]), Some((1, 8)));
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file2"]), Some((1, 12)));
        assert_eq!(lookup.remove_file(vec_str!["folder1", "subfolder1", "file2"]), Some((1, 12)));
        lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 16), 1);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file1"]), Some((1, 16)));
        assert_eq!(lookup.remove_file(vec_str!["folder1", "subfolder1", "file1"]), Some((1, 16)));


    }

    #[test]
    fn remove_folders() {
        let mut lookup = IDLookup::new();

        lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 13), 1);
        lookup.add_file(vec_str!["folder1", "subfolder1", "file2"], (1, 12), 1);
        lookup.add_file(vec_str!["folder1", "subfolder2", "file3"], (1, 11), 1);
        lookup.add_file(vec_str!["folder2", "subfolder1", "file4"], (1, 10), 1);
        lookup.add_file(vec_str!["folder2", "file5"], (1, 9), 1);
        lookup.add_file(vec_str!["file6"], (1, 8), 1);

        let remove_list = lookup.remove_folder(vec_str!["folder1", "subfolder1"]);
        assert!(remove_list.contains(&(1, 12)));
        assert!(remove_list.contains(&(1, 13)));
        assert_eq!(remove_list.len(), 2);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file1"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file2"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder2", "file3"]), Some((1, 11)));
        assert_eq!(lookup.remove_folder(vec_str!["folder1"]), vec![(1, 11)]);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file1"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder1", "file2"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder1", "subfolder2", "file3"]), None);

        assert_eq!(lookup.get_id_for(vec_str!["folder2", "subfolder1", "file4"]), Some((1, 10)));
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "file5"]), Some((1, 9)));
        assert_eq!(lookup.get_id_for(vec_str![ "file6"]), Some((1, 8)));
        assert_eq!(lookup.get_id_for(vec_str![ "file1"]), None);
        assert_eq!(lookup.get_id_for(vec_str![ "file5"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "subfolder1", "file5"]), None);
        assert_eq!(lookup.get_id_for(vec_str!["folder2", "subfolder1", "subsubfolder1", "file5"]), None);
        assert_eq!(lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 14), 1), "file1".to_string());
        assert_eq!(lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (1, 15), 1), "file1(site 1)".to_string());
        assert_eq!(lookup.add_file(vec_str!["folder1", "subfolder1", "file1"], (2, 16), 2), "file1(site 2)".to_string());
    }
}
