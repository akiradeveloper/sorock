use super::{Ballot, Entry};
use crate::Index;
use anyhow::Result;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tokio::fs;

fn extract_entry_index(path: &Path) -> Index {
    let name = path.file_name().unwrap();
    let name = name.to_str().unwrap();
    name.parse().unwrap()
}

pub struct Storage {
    root_dir: PathBuf,
}
impl Storage {
    fn ballot_path(&self) -> PathBuf {
        self.root_dir.join("ballot")
    }
    fn entry_path(&self, i: Index) -> PathBuf {
        self.root_dir.join("entry").join(format!("{i}"))
    }
    async fn entries(&self) -> Result<BTreeSet<Index>> {
        let root_dir = self.root_dir.join("entry");
        let mut dir_iter = tokio::fs::read_dir(root_dir).await?;
        let mut out = BTreeSet::new();
        while let Some(entry) = dir_iter.next_entry().await? {
            let idx = extract_entry_index(&entry.path());
            out.insert(idx);
        }
        Ok(out)
    }
    pub fn destory(root_dir: &Path) -> Result<()> {
        std::fs::remove_dir_all(root_dir).ok();
        Ok(())
    }
    /// Create the initial state.
    /// You should call `destory` before calling this function.
    pub fn create(root_dir: &Path) -> Result<()> {
        std::fs::create_dir(root_dir)?;
        std::fs::create_dir(root_dir.join("entry"))?;
        let init_ballot = Ballot::new();
        let init_ballot: Vec<u8> = init_ballot.into();
        let ballot_path = root_dir.join("ballot");
        std::fs::write(ballot_path, init_ballot)?;
        Ok(())
    }
    pub fn open(root_dir: &Path) -> Result<Self> {
        Ok(Self {
            root_dir: root_dir.to_owned(),
        })
    }
}
#[async_trait::async_trait]
impl super::RaftStorage for Storage {
    async fn insert_entry(&self, i: Index, e: Entry) -> Result<()> {
        let path = self.entry_path(i);
        let bin: Vec<u8> = e.into();
        tokio::fs::write(path, bin).await?;
        Ok(())
    }
    async fn delete_entry(&self, i: Index) -> Result<()> {
        let path = self.entry_path(i);
        fs::remove_file(&path).await?;
        Ok(())
    }
    async fn get_entry(&self, i: Index) -> Result<Option<Entry>> {
        let path = self.entry_path(i);
        if !path.exists() {
            return Ok(None);
        }
        let bin = tokio::fs::read(&path).await?;
        let entry = Entry::from(bin);
        Ok(Some(entry))
    }
    async fn get_head_index(&self) -> Result<Index> {
        let entries = self.entries().await?;
        let r = match entries.iter().next() {
            Some(k) => *k,
            None => 0,
        };
        Ok(r)
    }
    async fn get_last_index(&self) -> Result<Index> {
        let entries = self.entries().await?;
        let r = match entries.iter().next_back() {
            Some(k) => *k,
            None => 0,
        };
        Ok(r)
    }
    async fn save_ballot(&self, v: Ballot) -> Result<()> {
        let path = self.ballot_path();
        let bin: Vec<u8> = v.into();
        tokio::fs::write(path, bin).await?;
        Ok(())
    }
    async fn load_ballot(&self) -> Result<Ballot> {
        let path = self.ballot_path();
        let bin = tokio::fs::read(path).await?;
        Ok(Ballot::from(bin))
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::storage;

    #[test]
    fn test_extract_entry_index() {
        let path1 = Path::new("/root/entry/10");
        assert_eq!(extract_entry_index(&path1), 10);

        let path2 = Path::new("/root/entry/10000000000000");
        assert_eq!(extract_entry_index(&path2), 10000000000000);
    }

    #[tokio::test]
    #[serial]
    async fn test_file_storage() -> Result<()> {
        let _ = std::fs::create_dir("/tmp/lol");
        let path = Path::new("/tmp/lol/file1.db");
        Storage::destory(&path).unwrap();
        Storage::create(&path).unwrap();
        let s = Storage::open(&path).unwrap();

        storage::test_storage(s).await?;

        Storage::destory(&path).unwrap();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_file_storage_persistency() -> Result<()> {
        let _ = std::fs::create_dir("/tmp/lol");
        let path = Path::new("/tmp/lol/file2.db");
        Storage::destory(&path).unwrap();
        Storage::create(&path).unwrap();

        let s = Storage::open(&path).unwrap();
        storage::persistency::test_pre_close(s).await?;

        let s = Storage::open(&path).unwrap();
        storage::persistency::test_post_close(s).await?;

        Storage::destory(&path).unwrap();
        Ok(())
    }
}
