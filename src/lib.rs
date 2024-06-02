use std::{
    collections::BTreeMap, fmt, fs::{self, File}, io::{self, BufRead, BufReader, Seek, Write},
    path
};

use serde::{Serialize, Deserialize};
use serde_json;


pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug)]
pub enum KvError {
    // my errors
    InvalidCommandError,
    InvalidKeyError,
    KeyNotFoundError,
    // embedded errors
    IoError(std::io::Error),
    SerdeJsonError(serde_json::Error)
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(e) => {
                e.fmt(f)
            },
            Self::SerdeJsonError(e) => {
                e.fmt(f)
            }
            Self::InvalidCommandError => {
                write!(f, "Invalid command found at offset for get operation")
            },
            Self::InvalidKeyError => {
                write!(f, "Invalid key found at offset for get operation")
            },
            Self::KeyNotFoundError => {
                write!(f, "Key not found")
            }
        }
    }
}

impl From<io::Error> for KvError {
    fn from(value: io::Error) -> Self {
        KvError::IoError(value)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(value: serde_json::Error) -> Self {
        KvError::SerdeJsonError(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Op {
    Set(String,String),
    Rm(String),
    Get(String),
}


pub struct KvStore {
    index: BTreeMap<String,u64>,
    log_file: path::PathBuf,
    log_size: u64
}

impl KvStore {
    pub fn open(path: impl Into<path::PathBuf>) -> Result<KvStore> {
        let mut dirpath = path.into().clone();
        dirpath.push("store");

        let kv_store = KvStore{index: BTreeMap::new(), log_file: dirpath, log_size: 0};

        if let Err(e) = File::open(&kv_store.log_file) {
            if let io::ErrorKind::NotFound = e.kind(){
                File::create(&kv_store.log_file)?;
            } else {
                return Err(KvError::IoError(e));
            }
        }

        kv_store.construct_index()
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.print_index();
        //operation
        let op = Op::Set(key, value);

        // serialize operation
        let mut serialized_op = serde_json::to_vec(& op)?;
        serialized_op.extend_from_slice("\n".as_bytes());
        let mut file = fs::OpenOptions::new().append(true).create(true).open(&self.log_file)?;
        let offset = file.seek(io::SeekFrom::End(0))?;

        self.log_size += serialized_op.len() as u64;
        file.write(& serialized_op.as_slice())?;
        file.flush()?;

        // update index
        if let Op::Set(k,_) = op {
            self.index.insert(k, offset);
        }

        self.compact()?;
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.index.get(&key) {
            let mut file = File::open(& self.log_file)?;
            file.seek(io::SeekFrom::Start(*offset))?;

            let mut buf_reader = io::BufReader::new( &mut file);

            let mut line = String::new();
            buf_reader.read_line(&mut line)?;

            let op = serde_json::from_slice::<Op>(line.as_bytes())?;

            if let Op::Set(k, v) = op {
                if k == key {
                    return Ok(Some(v))
                } else {
                    return Err(KvError::InvalidKeyError)
                }
            } else {
                Err(KvError::InvalidCommandError)
            }
        } else {
            Ok(None)
        }
        
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.print_index();
        if let Some(_) = self.index.get(&key) {

            // serialize operation
            let op = Op::Rm(key);
            let mut serialized_op = serde_json::to_vec(& op)?;
            serialized_op.extend_from_slice("\n".as_bytes());

            // seek to end
            let mut file = fs::OpenOptions::new().append(true).create(true).open(&self.log_file)?;

            self.log_size = serialized_op.len() as u64;
            // write to disk
            file.write(& serialized_op.as_slice())?;
            file.flush()?;
            // update index
            if let Op::Rm(k) = op {
                self.index.remove(&k);
            }
            self.compact()?;
            return Ok(())
        } else {
            return Err(KvError::KeyNotFoundError);
        }
    }

    fn construct_index(mut self) -> Result<Self> {
        let mut offset = 0;
        let mut file_handle = File::open(&self.log_file)?;
        let buf_reader = io::BufReader::new( &mut file_handle);
        for line in buf_reader.lines() {
            let content = line.unwrap();
            // parse line
            match serde_json::from_slice::<Op>(content.as_bytes())? {
                Op::Set(k, _) => {
                    self.index.insert(k, offset);
                },
                Op::Rm(k) => {
                    self.index.remove(&k);
                },
                _ => ()
            }

            offset += content.len() as u64 + 1;
        }

        Ok(self)
    }

    fn compact(&mut self) -> Result<()> {

        if self.log_size >= 1024*1024 {
            let mut content = String::new();
            {
                let file_handle = File::open(&self.log_file)?;
                let mut buf = BufReader::new(file_handle);
                for i in self.index.iter() {
                    buf.seek(io::SeekFrom::Start(*i.1))?;
                    buf.read_line(&mut content)?;
                }
            }
            
            let mut file = File::create(&self.log_file)?;
            file.write(content.as_bytes())?;
            file.flush()?;
            self.log_size = content.len() as u64;
            Ok(())
        } else {
            Ok(())
        }
    }
}
