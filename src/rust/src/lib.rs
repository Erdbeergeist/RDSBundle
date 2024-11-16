use extendr_api::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use zstd::stream::{copy_decode, copy_encode};

//Compress stream
fn compress_data(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut comp_data: Vec<u8> = Vec::new();
    copy_encode(input, &mut comp_data, 0)?;

    Ok(comp_data)
}

//Decompress stream
fn decompress_data(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut uncomp_data: Vec<u8> = Vec::new();
    copy_decode(input, &mut uncomp_data)?;

    Ok(uncomp_data)
}

/// Write the compressed data
#[extendr]
fn write_data_object(
    file_path: String,
    ser_obj: Vec<u8>,
    offset: usize,
) -> extendr_api::Result<Robj> {
    let serialized_object: Vec<u8> = ser_obj.try_into().unwrap();

    let compressed_object = match compress_data(&serialized_object) {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error compressing: {}",
                e
            )))
        }
    };

    let mut file = match File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&file_path)
    {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error opening file: {}",
                e
            )))
        }
    };

    file.seek(SeekFrom::Start(offset as u64))
        .map_err(|e| Robj::from(format!("Error seeking in file: {}", e)));

    file.write_all(&compressed_object)
        .map_err(|e| Robj::from(format!("Error writing in file: {}", e)));

    Ok(Robj::from(compressed_object.len()))
}

#[extendr]
fn read_data_object(file_path: String, offset: usize, size: usize) -> extendr_api::Result<Robj> {
    let mut file = match File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&file_path)
    {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error opening file: {}",
                e
            )))
        }
    };

    let mut compressed_object = vec![0u8; size];

    match file.read_exact(&mut compressed_object) {
        Ok(_) => {}
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error reading from file: {}",
                e
            )))
        }
    };

    let decompressed_object = match decompress_data(&compressed_object) {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error decompressing: {}",
                e
            )))
        }
    };
    Ok(Robj::from(decompressed_object))
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod rdsb;
    fn write_data_object;
    fn read_data_object;
}
