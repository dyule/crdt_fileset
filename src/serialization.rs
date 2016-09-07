use {FileSet, FileUpdater, FileMetadata};
use lookup::IDLookup;
use std::collections::hash_map::HashMap;
use std::io;
use std::path::PathBuf;
use byteorder::{NetworkEndian, ByteOrder};

impl<FU: FileUpdater> FileSet<FU> {

    pub fn compress_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut int_buf = [0;4];
        NetworkEndian::write_u32(&mut int_buf, self.last_timestamp);
        try!(writer.write(&int_buf));
        NetworkEndian::write_u32(&mut int_buf, self.last_id);
        try!(writer.write(&int_buf));
        NetworkEndian::write_u32(&mut int_buf, self.site_id);
        try!(writer.write(&int_buf));
        NetworkEndian::write_u32(&mut int_buf, self.files.len() as u32);
        try!(writer.write(&int_buf));
        for (&(site_id, id), file) in self.files.iter() {
            NetworkEndian::write_u32(&mut int_buf, site_id);
            try!(writer.write(&int_buf));
            NetworkEndian::write_u32(&mut int_buf, id);
            try!(writer.write(&int_buf));
            NetworkEndian::write_u32(&mut int_buf, file.filename.0);
            try!(writer.write(&int_buf));
            NetworkEndian::write_u32(&mut int_buf, file.filename.1.len() as u32);
            try!(writer.write(&int_buf));
            for filename in file.filename.1.iter() {
                let bytes = filename.as_bytes();
                NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
                try!(writer.write(&int_buf));
                try!(writer.write(bytes));
            }
            let bytes = file.printed_filename.as_bytes();
            NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
            try!(writer.write(&int_buf));
            try!(writer.write(bytes));
            NetworkEndian::write_u32(&mut int_buf, file.attributes.len() as u32);
            try!(writer.write(&int_buf));
            for (key, &(time_stamp, ref value)) in file.attributes.iter() {
                let bytes = key.as_bytes();
                NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
                try!(writer.write(&int_buf));
                try!(writer.write(bytes));
                NetworkEndian::write_u32(&mut int_buf, time_stamp);
                try!(writer.write(&int_buf));
                let bytes = value.as_bytes();
                NetworkEndian::write_u32(&mut int_buf, bytes.len() as u32);
                try!(writer.write(&int_buf));
                try!(writer.write(bytes));
            }
        }
        Ok(())
    }

    pub fn expand_from<R: io::Read>(reader: &mut R, updater: FU, storage_path: PathBuf) -> io::Result<FileSet<FU>> {
        trace!("Expanding Fileset");
        let mut int_buf = [0;4];
        try!(reader.read_exact(&mut int_buf));
        let last_timestamp = NetworkEndian::read_u32(&int_buf);
        trace!("last_timestamp: {}", last_timestamp);
        try!(reader.read_exact(&mut int_buf));
        let last_id = NetworkEndian::read_u32(&int_buf);
        trace!("last_id: {}", last_id);
        try!(reader.read_exact(&mut int_buf));
        let site_id = NetworkEndian::read_u32(&int_buf);
        trace!("site_id: {}", site_id);
        try!(reader.read_exact(&mut int_buf));
        let file_count = NetworkEndian::read_u32(&int_buf) as usize;
        trace!("file count: {}", file_count);
        let mut files = HashMap::with_capacity(file_count);
        let mut id_lookup = IDLookup::new();
        for _ in 0..file_count {
            try!(reader.read_exact(&mut int_buf));
            let file_site_id = NetworkEndian::read_u32(&int_buf);
            trace!("file site_id: {}", file_site_id);
            try!(reader.read_exact(&mut int_buf));
            let id = NetworkEndian::read_u32(&int_buf);
            trace!("id: {}", id);
            try!(reader.read_exact(&mut int_buf));
            let filename_timestamp = NetworkEndian::read_u32(&int_buf);
            trace!("filename_timestamp: {}", filename_timestamp);
            try!(reader.read_exact(&mut int_buf));
            let filename_component_count = NetworkEndian::read_u32(&int_buf) as usize;
            let mut filename = Vec::with_capacity(filename_component_count);
            for _ in 0..filename_component_count {
                filename.push(read_str(reader, &mut int_buf).unwrap())
            }
            trace!("filename: {:?}", filename);
            let printed_filename = try!(read_str(reader, &mut int_buf));
            trace!("printed_filename: {}", printed_filename);
            try!(reader.read_exact(&mut int_buf));
            let attribute_count = NetworkEndian::read_u32(&int_buf) as usize;
            trace!("attribute_count: {}", attribute_count);
            let mut attributes = HashMap::with_capacity(attribute_count);
            for _ in 0..attribute_count {
                let key = try!(read_str(reader, &mut int_buf));
                try!(reader.read_exact(&mut int_buf));
                let attribute_timestamp = NetworkEndian::read_u32(&int_buf);
                let value = try!(read_str(reader, &mut int_buf));
                attributes.insert(key, (attribute_timestamp, value));
            }
            let metadata = FileMetadata{
                filename: (filename_timestamp, filename),
                printed_filename: printed_filename.clone(),
                attributes: attributes
            };
            id_lookup.add_file(metadata.get_local_filename().iter(), (file_site_id, id), file_site_id);
            files.insert((file_site_id, id), metadata);

        }
        trace!("Fileset loaded");
        Ok(FileSet {
            files: files,
            id_lookup: id_lookup,
            updater: updater,
            last_timestamp: last_timestamp,
            last_id: last_id,
            site_id: site_id,
            storage_path: storage_path
        })
    }

}


fn read_str<R: io::Read>(reader: &mut R, int_buf: &mut [u8;4]) -> io::Result<String> {
    try!(reader.read_exact(int_buf));
    let str_len = NetworkEndian::read_u32(int_buf) as usize;
    let mut str_vec:Vec<u8> = Vec::with_capacity(str_len);
    str_vec.resize(str_len, 0);
    try!(reader.read_exact(&mut str_vec));
    Ok(String::from_utf8_lossy(str_vec.as_slice()).into_owned())
}
