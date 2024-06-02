extern crate kvs;

use std::process::exit;

use clap::{Parser, Subcommand};
use kvs::KvError;
use serde::{Serialize, Deserialize};

#[derive(Debug,Parser)]
#[command(version, about, long_about=None)]
#[command(propagate_version=true)]
struct Cli {
    #[command(subcommand)]
    command: Commands
}

#[derive(Debug,Subcommand,Serialize,Deserialize)]
enum Commands {
    Get{key: String},
    Set{key: String, value: String},
    Rm{key: String},
}

impl Into<kvs::Op> for Commands {
    fn into(self) -> kvs::Op {
        match self {
            Self::Get { key } => kvs::Op::Get(key),
            Self::Set { key, value } => kvs::Op::Set(key, value),
            Self::Rm { key } => kvs::Op::Rm(key)
        }
    }
}

fn main() {
    let cli = Cli::parse();

    let dir = std::env::current_dir().unwrap();
    let mut store = kvs::KvStore::open(dir).unwrap();
    match cli.command  {
        Commands::Get { key } => {
            let result = store.get(key).unwrap();
            match result {
                Some(value) => {
                    println!("{value}");
                },
                None => {
                    println!("Key not found");
                }
            }
        },
        Commands::Rm { key } => {
            match store.remove(key) {
                Ok(()) => (),
                Err(KvError::KeyNotFoundError) => {
                    println!("Key not found");
                    exit(1);
                },
                Err(e) => {
                    panic!("{e}");
                }
            }
        },
        Commands::Set { key, value } => {
            store.set(key, value).unwrap();
        }
    }
}
