extern crate aws_sdk_s3;

use aws_sdk_s3::operation::{
    get_object::{GetObjectOutput},
    list_objects_v2::ListObjectsV2Output,
    delete_object::{DeleteObjectOutput},
    put_object::{PutObjectOutput},
};

use std::fs::{File, OpenOptions};
use std::io::{Write, Read};
use std::path::Path;

use super::errors::*;

#[derive(Debug)]
pub struct Key {
    pub bucket: String,
    pub key: String,
}

pub async fn get(s3: &aws_sdk_s3::Client, o: &Key, f: &Path) -> Result<GetObjectOutput> {
    let req = s3
        .get_object()
        .bucket(o.bucket.to_owned())
        .key(o.key.to_owned());
    let mut result = req
        .send()
        .await
        .chain_err(|| "couldn't get item")?;

    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(f)
        .chain_err(|| "open failed")?;
    while let Some(bytes) = result.body.try_next().await.chain_err(|| "unable to get partial body")? {
        target.write(&bytes).chain_err(|| "unable to write")?;
    }

    Ok(result)
}

pub async fn put(s3: &aws_sdk_s3::Client, f: &Path, o: &Key) -> Result<PutObjectOutput> {
    let mut f = File::open(f).chain_err(|| "open failed")?;
    let mut contents: Vec<u8> = Vec::new();
    f.read_to_end(&mut contents).chain_err(|| "read failed")?;
    let req = s3
        .put_object()
        .bucket(o.bucket.to_owned())
        .key(o.key.to_owned())
        .body(contents.into());

    req
        .send()
        .await
        .chain_err(|| "Couldn't PUT object")
}

pub async fn del(s3: &aws_sdk_s3::Client, o: &Key) -> Result<DeleteObjectOutput> {
    let req = s3
        .delete_object()
        .bucket(o.bucket.to_owned())
        .key(o.key.to_owned());
    req
        .send()
        .await
        .chain_err(|| "Couldn't DELETE object")
}

pub async fn list(s3: &aws_sdk_s3::Client, k: &Key) -> Result<ListObjectsV2Output> {
    let req = s3
        .list_objects_v2()
        .bucket(k.bucket.to_owned())
        .prefix(k.key.to_owned());
    req
        .send()
        .await
        .chain_err(|| "Couldn't list items in bucket")
}
