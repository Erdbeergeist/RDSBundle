use extendr_api::prelude::*;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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
    mut ser_obj: Robj,
    offset: u64,
    index_size: i16,
) -> extendr_api::Result<Robj> {
    let serialized_object: &[u8] = ser_obj.as_raw_slice_mut().unwrap();

    let compressed_object = match compress_data(&serialized_object) {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error compressing: {}",
                e
            )))
        }
    };

    let file = match File::options()
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

    let mut writer = BufWriter::with_capacity(compressed_object.len() * 2, file);

    // if we are here then index size was not passed as negative indicating
    // the first append, check if we should skip the existing index table
    if (compressed_object.len() as i16 <= index_size) & (index_size > 0) {
        match writer.seek(SeekFrom::End(0)) {
            Ok(f) => println!("Index > compressed object, seeking to {}", f),
            Err(e) => {
                return Err(extendr_api::Error::from(format!(
                    "Error seeking in file: {}",
                    e
                )))
            }
        };
    } else {
        match writer.seek(SeekFrom::Start(offset as u64)) {
            Ok(f) => f,
            Err(e) => {
                return Err(extendr_api::Error::from(format!(
                    "Error seeking in file: {}",
                    e
                )))
            }
        };
    }
    match writer.write_all(&compressed_object) {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error writing to file: {}",
                e
            )))
        }
    };

    Ok(Robj::from(compressed_object.len()))
}

#[extendr]
fn read_data_object(file_path: String, offset: u64, size: usize) -> extendr_api::Result<Robj> {
    let file = match File::options().read(true).open(&file_path) {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error opening file: {}",
                e
            )))
        }
    };

    let mut reader = BufReader::with_capacity(size * 2, file);

    match reader.seek(SeekFrom::Start(offset)) {
        Ok(_) => {}
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error seeking in file: {}",
                e
            )))
        }
    };

    let mut compressed_object = vec![0u8; size];

    match reader.read_exact(&mut compressed_object) {
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

#[extendr]
fn read_data_object_to_R_buffer(
    mut r_buf: Robj,
    file_path: String,
    offset: u64,
    size: usize,
) -> extendr_api::Result<Robj> {
    if !r_buf.is_raw() {
        return Err(extendr_api::Error::Other(
            "Provided R object is not a raw vector".into(),
        ));
    }
    let file = match File::options().read(true).open(&file_path) {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error opening file: {}",
                e
            )))
        }
    };

    let mut reader = BufReader::with_capacity(size * 2, file);

    match reader.seek(SeekFrom::Start(offset)) {
        Ok(_) => {}
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error seeking in file: {}",
                e
            )))
        }
    };

    let mut compressed_object = r_buf.as_raw_slice_mut().unwrap();

    match reader.read_exact(&mut compressed_object) {
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
    Ok(Robj::from(offset))
}
// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod rdsb;
    fn write_data_object;
    fn read_data_object;
    fn read_data_object_to_R_buffer;
}
