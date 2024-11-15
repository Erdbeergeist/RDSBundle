use extendr_api::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
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
fn write_data_object(file_path: String, ser_obj: Robj, offset: usize) -> extendr_api::Result<Robj> {
    let serialized_object: Vec<u8> = ser_obj.try_into().unwrap();

    let compressed_object = compress_data(&serialized_object);

    let compressed_object = match compressed_object {
        Ok(f) => f,
        Err(e) => {
            return Err(extendr_api::Error::from(format!(
                "Error compressing: {}",
                e
            )))
        }
    };

    let mut file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .open(&file_path);

    let mut file = match file {
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

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod rdsb;
    fn write_data_object;
}
