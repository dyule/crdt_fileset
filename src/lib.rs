extern crate byteorder;

#[macro_use]
extern crate log;

mod serialization;
mod lookup;

use lookup::IDLookup;
use std::collections::hash_map::{HashMap, Entry};
use std::collections::btree_map::{BTreeMap};
use std::path::{Path, PathBuf};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::fmt;

pub type FileID = (u32, u32);

pub trait FileUpdater: fmt::Debug {
    type FileTransaction: fmt::Debug;
    fn create_file<P: AsRef<Path>>(&mut self, filename: P) -> io::Result<()>;
    fn remove_file<P: AsRef<Path>>(&mut self, filename: P) -> io::Result<()>;
    fn update_file<P: AsRef<Path>>(&mut self, filename: P, timestamp_lookup: &BTreeMap<u32, (u32, u32)>, transaction: &mut Self::FileTransaction) -> io::Result<()>;
    fn move_file<P: AsRef<Path>>(&mut self, old_filename: P, new_filename: P) -> io::Result<()>;
    fn get_local_changes<P: AsRef<Path>>(&mut self, filename: P) -> io::Result<(Self::FileTransaction, BTreeMap<u32, (u32, u32)>)>;
    fn get_changes_since<P: AsRef<Path>>(&self, filename: P, last_timestamp: Option<(u32, u32)>) -> Self::FileTransaction;
    fn get_base_path(&self) -> &Path;
}

#[derive(Debug)]
pub enum MetadataTransaction {
    Filename(Vec<String>),
    Custom(String, String),
}
pub struct FileSet<FU: FileUpdater> {
    files: HashMap<(u32, u32), FileMetadata>,
    id_lookup: IDLookup,
    updater: FU,
    last_timestamp: u32,
    last_id: u32,
    site_id: u32,
    storage_path: PathBuf
}

#[derive(Debug)]
pub struct FileMetadata {
    filename: (u32, Vec<String>),
    printed_filename: String,
    attributes: HashMap<String, (u32, String)>
}

pub struct FileHistory<FU: FileUpdater> {
    pub filename: (u32, Vec<String>),
    pub attributes: HashMap<String, (u32, String)>,
    pub operation_history: FU::FileTransaction
}

#[derive(Debug)]
pub struct State {
    pub time_stamp: u32,
    pub site_id: u32,
}

pub enum FileSetError {
    IOError(io::Error),
    IDNotFound(u32, u32)
}

#[derive(Debug)]
pub struct CreateOperation {
    pub state: State,
    pub filename: Vec<String>,
    pub id: FileID
}

#[derive(Debug)]
pub struct RemoveOperation {
    pub id: FileID
}

#[derive(Debug)]
pub struct UpdateOperation<FU: FileUpdater> {
    pub id: FileID,
    pub data: FU::FileTransaction
}

#[derive(Debug)]
pub struct UpdateMetadata {
    pub state: State,
    pub id: FileID,
    pub data: MetadataTransaction
}
#[derive(Debug)]
pub enum FileSetOperation<FU:FileUpdater> {
    Create(CreateOperation),
    Remove(RemoveOperation),
    Update(UpdateOperation<FU>, BTreeMap<u32, (u32, u32)>),
    UpdateMetadata(UpdateMetadata),
}

impl<FU: FileUpdater> FileHistory<FU> {
    #[inline]
    pub fn new(filename_timestamp: u32, filename: Vec<String>, attributes: HashMap<String, (u32, String)>, operations: FU::FileTransaction) -> FileHistory<FU> {
        FileHistory {
            filename: (filename_timestamp, filename),
            attributes: attributes,
            operation_history: operations
        }
    }
}

impl FileMetadata {
    fn get_local_filename(&self) -> PathBuf {
        let mut path = PathBuf::new();
        for component in self.filename.1[0..self.filename.1.len() - 1].iter() {
            path.push(&component);
        }
        path.push(&self.printed_filename);
        path
    }

    pub fn get_file_path(&self)-> &Vec<String> {
        &self.filename.1
    }
    pub fn get_file_timestamp(&self) -> u32 {
        self.filename.0
    }
}

impl<FU: FileUpdater> FileSet<FU> {
    pub fn new<P: AsRef<Path>>(updater: FU, site_id: u32, storage_path: P) -> io::Result<FileSet<FU>> {
        let storage_path = storage_path.as_ref().to_path_buf();
        match fs::File::open(storage_path.join("crdt").as_path()) {
            Ok(mut store_file) => {
                FileSet::expand_from(&mut store_file, updater, storage_path)
            },
            Err(_) => {
                Ok(FileSet{
                    files: HashMap::new(),
                    id_lookup: IDLookup::new(),
                    site_id: site_id,
                    last_timestamp: 0,
                    last_id: 0,
                    updater: updater,
                    storage_path: storage_path.to_path_buf()
                })
            }
        }
    }

    pub fn integrate_remote(&mut self, remote: FileSetOperation<FU>) -> Result<(), FileSetError> {
        let result = match remote {
            FileSetOperation::Create(o) => self.integrate_create(o),
            FileSetOperation::Remove(o) => self.integrate_remove(o),
            FileSetOperation::Update(mut o, lookup) => self.integrate_update(&mut o, &lookup),
            FileSetOperation::UpdateMetadata(o) => self.integrate_update_metadata(o),
        };
        self.save().unwrap();
        result

    }

    pub fn has_path(&self, path: &PathBuf) -> bool {
        self.id_lookup.get_id_for(path.iter()).is_some()
    }

    pub fn process_create(&mut self, path: &Path) -> FileSetOperation<FU> {
        trace!("Processing create on {:?}", path);
        let path = path.to_path_buf();
        let filename: Vec<&OsStr> = path.into_iter().collect();
        let id = self.get_next_id();
        let state = self.create_state();
        let printed = self.id_lookup.add_file(filename.clone().into_iter(), (self.site_id, id), self.site_id);
        let filename:Vec<_> = filename.iter().map(|c| c.to_str().unwrap().to_string()).collect();
        self.files.insert((self.site_id, id), FileMetadata {
            filename: (state.time_stamp, filename.clone()),
            printed_filename: printed,
            attributes: HashMap::new()
        });
        self.save().unwrap();
        FileSetOperation::Create(CreateOperation {
            state: state,
            id: (self.site_id, id),
            filename: filename
        })
    }

    pub fn process_remove(&mut self, path: &Path) -> FileSetOperation<FU> {
        trace!("Processing remove on {:?}", path);
        let (site_id, id) = self.id_lookup.remove_file(path).unwrap();
        self.files.remove(&(self.site_id, id));
        self.save().unwrap();
        FileSetOperation::Remove(RemoveOperation {
            id: (site_id, id),
        })
    }

    pub fn process_remove_folder(&mut self, path: &Path) -> Vec<FileSetOperation<FU>> {
        trace!("Processing remove on {:?}", path);
        let ids = self.id_lookup.remove_folder(path);
        for id in ids.iter() {
            self.files.remove(id);
        }
        self.save().unwrap();
        ids.into_iter().map(|id| FileSetOperation::Remove(RemoveOperation{
            id: id
        })).collect()
    }

    pub fn process_update(&mut self, path: &Path, transaction: FU::FileTransaction, timestamp_lookup: BTreeMap<u32, (u32, u32)>) -> FileSetOperation<FU> {
        trace!("Processing update on {:?}", path);
        let (site_id, id) = self.id_lookup.get_id_for(path).unwrap();
        self.save().unwrap();
        FileSetOperation::Update(UpdateOperation{
            id: (site_id, id),
            data: transaction
        }, timestamp_lookup)
    }

    pub fn process_file_move(&mut self, old_path: &Path, new_path: &Path) -> FileSetOperation<FU> {
        trace!("Processing file_move on {:?}", old_path);
        let (site_id, id) = self.id_lookup.remove_file(old_path).unwrap();
        let state = self.create_state();
        let printed = self.id_lookup.add_file(new_path, (site_id, id), site_id);
        let filename:Vec<_> = new_path.iter().map(|c| c.to_str().unwrap().to_string()).collect();
        {
            let metadata = self.files.get_mut(&(site_id, id)).unwrap();
            metadata.filename = (state.time_stamp, filename.clone());
            metadata.printed_filename = printed;
        }
        self.save().unwrap();
        FileSetOperation::UpdateMetadata(UpdateMetadata {
            state: state,
            id: (site_id, id),
            data: MetadataTransaction::Filename(filename)
        })
    }

    pub fn get_changes_since(&self, timestamp: Option<(u32, u32)>) -> HashMap<(u32, u32), FileHistory<FU>> {
        self.files.iter().map(|(&key, file_metadata)| {
            (key, FileHistory {
                filename: file_metadata.filename.clone(),
                attributes: file_metadata.attributes.clone(),
                operation_history: self.updater.get_changes_since(file_metadata.get_local_filename().as_path(), timestamp)
            })
        }).collect()
    }

    pub fn get_all_files(&self) -> &HashMap<(u32, u32), FileMetadata> {
        &self.files
    }

    pub fn get_file_history_for(&self, file: (u32, u32)) -> Option<FU::FileTransaction> {
        if let Some(file_metadata) = self.files.get(&file) {
            Some(self.updater.get_changes_since(file_metadata.get_local_filename().as_path(), None))
        } else {
            None
        }
    }

    pub fn integrate_remote_file_list(&mut self, mut file_list: HashMap<(u32, u32), FileHistory<FU>>, timestamp_lookup: BTreeMap<u32, (u32, u32)>) -> Vec<FileSetOperation<FU>> {
        // Recursively go through every file in the directory
        // If the file is in the local list,
        //      If the file is also in the remote list, then process local changes
        // Otherwise, create the file in the list, and process the local changes
        let mut operations = Vec::new();
        let base_path = self.updater.get_base_path().to_path_buf();
        self.scan_dir(base_path.as_path(), base_path.as_path(), &mut file_list, &timestamp_lookup, &mut operations).unwrap();
        // For each file in the local list, if it is not in the remote list, then delete the file in the local list and on the file system
        trace!("Current files are: {:?}", self.files);
        let mut new_file_list = HashMap::new();
        for ((site_id, id), file) in self.files.drain() {
            if file_list.contains_key(&(site_id, id)) {
                new_file_list.insert((site_id, id), file);
            } else {
                let filename = file.get_local_filename();
                self.id_lookup.remove_file(filename.iter());
                self.updater.remove_file(filename).unwrap();
            }
        }
        self.files = new_file_list;

        // For each file in the remote list, if it is not in the local list, then create it in the local list and on the file system
        for  ((site_id, id), mut file_history) in file_list.into_iter() {
            if !self.files.contains_key(&(site_id, id)) {
                let printed = self.id_lookup.add_file(file_history.filename.1.iter().map(OsStr::new), (site_id, id), site_id);
                let file = FileMetadata {
                    filename: file_history.filename,
                    printed_filename: printed,
                    attributes: file_history.attributes.clone() // TODO consider retrieving these separately when they are needed
                };
                let actual_filename = file.get_local_filename();
                self.files.insert((site_id, id), file);
                self.updater.create_file(&actual_filename).unwrap();
                self.updater.update_file(&actual_filename, &timestamp_lookup, &mut file_history.operation_history).unwrap();
            }
        }
        self.save().unwrap();
        operations
    }



}

impl<FU: FileUpdater> FileSet<FU>  {

    fn create_state(&mut self) -> State {
        let timestamp = self.last_timestamp;
        self.last_timestamp += 1;
        State {
            site_id: self.site_id,
            time_stamp: timestamp
        }
    }
    fn get_next_id(&mut self) -> u32 {
        let id = self.last_id;
        self.last_id += 1;
        id
    }

    fn integrate_create(&mut self, o: CreateOperation) -> Result<(), FileSetError> {
        let actual_filename = self.id_lookup.add_file(o.filename.iter().map(OsStr::new), o.id, o.id.0);
        let metadata = FileMetadata{
            filename: (o.state.time_stamp, o.filename),
            printed_filename: actual_filename,
            attributes: HashMap::new()
        };
        let path = metadata.get_local_filename();
        self.files.insert(o.id, metadata);
        self.updater.create_file(&path).map_err(|e| {FileSetError::IOError(e)})
    }


    fn integrate_remove(&mut self, o: RemoveOperation) -> Result<(), FileSetError> {
        let metadata = match self.files.remove(&o.id) {
            Some(md) => md,
            None => {return Err(FileSetError::IDNotFound(o.id.0, o.id.1))}
        };
        let filename = metadata.get_local_filename();
        self.id_lookup.remove_file(&filename);
        self.updater.remove_file(filename).map_err(|e| {FileSetError::IOError(e)})
    }

    fn integrate_update(&mut self, o: &mut UpdateOperation<FU>, timestamp_lookup: &BTreeMap<u32, (u32, u32)>) -> Result<(), FileSetError> {
        let metadata = match self.files.get(&o.id) {
            Some(md) => md,
            None => {return Err(FileSetError::IDNotFound(o.id.0, o.id.1))}
        };
        self.updater.update_file(&metadata.get_local_filename(), timestamp_lookup, &mut o.data).map_err(|e| {FileSetError::IOError(e)})
    }

    fn integrate_update_metadata(&mut self, o: UpdateMetadata) -> Result<(), FileSetError> {
        {

            match o.data{
                MetadataTransaction::Filename(filename) => {
                    let (old_filename, new_filename) = {
                        let metadata = match self.files.get_mut(&o.id) {
                            Some(md) => md,
                            None => {return Err(FileSetError::IDNotFound(o.id.0, o.id.1))}
                        };
                        if metadata.filename.0 > o.state.time_stamp || metadata.filename.0 == o.state.time_stamp && self.site_id > o.state.site_id {
                            return Ok(())
                        }
                        let old_filename = metadata.get_local_filename();
                        self.id_lookup.remove_file(old_filename.iter());
                        let actual_filename = self.id_lookup.add_file(filename.iter().map(OsStr::new), o.id, o.state.site_id);
                        metadata.filename = (o.state.time_stamp, filename);
                        metadata.printed_filename = actual_filename;
                        (old_filename, metadata.get_local_filename())
                    };
                    self.updater.move_file(&old_filename, &new_filename).map_err(|e| {FileSetError::IOError(e)})
                },
                MetadataTransaction::Custom(key, value) => {
                    let metadata = match self.files.get_mut(&o.id) {
                        Some(md) => md,
                        None => {return Err(FileSetError::IDNotFound(o.id.0, o.id.1))}
                    };
                    match metadata.attributes.entry(key) {
                        Entry::Occupied(ref mut entry) => {
                            {
                                let val = entry.get();
                                if val.0 > o.state.time_stamp || val.0 == o.state.time_stamp && self.site_id > o.state.site_id {
                                    return Ok(())
                                }
                            }
                            entry.insert((o.state.time_stamp, value));
                            Ok(())
                        },
                        Entry::Vacant(entry) => {
                            entry.insert((o.state.time_stamp, value));
                            Ok(())
                        }
                    }

                }
            }
        }
    }

    fn scan_dir(&mut self, base_path: &Path, actual_path: &Path, remote_files: &mut HashMap<(u32, u32), FileHistory<FU>>, timestamp_lookup: &BTreeMap<u32, (u32, u32)>, operations: &mut Vec<FileSetOperation<FU>>) -> io::Result<()> {
        trace!("Scanning directory {:?}", actual_path);
        if actual_path.starts_with(&self.storage_path) {
            return Ok(())
        }
        for entry in try!(fs::read_dir(actual_path)) {
            let entry = try!(entry);
            let path = entry.path();
            if path.is_dir() {
                try!(self.scan_dir(base_path, path.as_path(), remote_files, timestamp_lookup, operations));
            } else {
                try!(self.check_for_file(base_path, path.as_path(), remote_files, timestamp_lookup, operations));
            }
        }
        trace!("Directory {:?} complete", actual_path);
        Ok(())
    }

    fn check_for_file(&mut self, base_path: &Path, actual_path: &Path, remote_files: &mut HashMap<(u32, u32), FileHistory<FU>>, timestamp_lookup: &BTreeMap<u32, (u32, u32)>, operations: &mut Vec<FileSetOperation<FU>>) -> io::Result<()> {
        trace!("Checking file {:?}", actual_path);
        let relative_path = actual_path.strip_prefix(base_path).unwrap();
        match self.id_lookup.get_id_for(relative_path) {
            Some((site_id, id)) => {
                if let Some(remote_file) = remote_files.get_mut(&(site_id, id)) {
                    trace!("Getting local changes");
                    let (local_changes, local_timestamps) = try!(self.updater.get_local_changes(relative_path));
                    operations.push(FileSetOperation::Update(UpdateOperation {
                        id: (site_id, id),
                        data: local_changes
                    }, local_timestamps));
                    trace!("Updating the file with remote operations");
                    try!(self.updater.update_file(&relative_path, timestamp_lookup, &mut remote_file.operation_history))
                }
            }, None => {
                operations.push(self.process_create(relative_path));
                if fs::metadata(actual_path).unwrap().len() > 0 {
                    let mut id = (0, 0);
                    if let Some(&FileSetOperation::Create(ref co)) = operations.get(operations.len() - 1)
                    {
                        id = co.id
                    }
                    let (local_changes, local_lookup) = try!(self.updater.get_local_changes(relative_path));
                    operations.push(FileSetOperation::Update(UpdateOperation {
                        id: id,
                        data: local_changes
                    }, local_lookup));

                }
            }
        }
        trace!("File {:?} complete", actual_path);
        Ok(())
    }


        fn save(&self) -> io::Result<()> {
            let store_path = self.storage_path.join("crdt");
            trace!("Saving fileset to {:?}", store_path);
            let mut store_file = try!(fs::File::create(store_path.as_path()));
            try!(self.compress_to(&mut store_file));
            Ok(())
        }



}

impl<FU:FileUpdater> fmt::Debug for FileSet<FU> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // files: HashMap<(u32, u32), FileMetadata>,
        // id_lookup: HashMap<String, (u32, u32)>,
        // updater: FU,
        // last_timestamp: u32,
        // last_id: u32,
        // site_id: u32,
        // storage_path: PathBuf
        try!(writeln!(f, "files: {:?}", self.files));
        writeln!(f, "last_timestamp: {:?}, last_id: {:?}", self.last_timestamp, self.last_id)
    }
}
