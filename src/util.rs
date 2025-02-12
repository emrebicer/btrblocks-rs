use datafusion::arrow::{
    array::{Array, Float64Array, Int32Array, StringArray},
    datatypes::DataType,
};
use object_store::{parse_url, path::Path, ObjectStore, ObjectStoreScheme};
use url::Url;

use crate::{error::BtrBlocksError, Result};

pub fn string_to_btr_url(btr_url: &mut String) -> Result<Url> {
    // Since btr compression ends up in a folder structure, the ending must be a slash
    ensure_slash_ending(btr_url);
    ensure_protocol(btr_url);

    Url::parse(btr_url).map_err(|err| BtrBlocksError::Url(err.to_string()))
}

pub fn ensure_slash_ending(url: &mut String) {
    if !url.ends_with("/") {
        url.push('/');
    };
}

pub fn ensure_protocol(url: &mut String) {
    // The Url expects a protocol, if there is no protocol found in the passed in string,
    // assume this is a local file
    if !url.starts_with("file://")
        && !url.starts_with("s3://")
        && !url.starts_with("gs://")
        && !url.starts_with("gcs://")
        && !url.starts_with("http://")
        && !url.starts_with("https://")
    {
        url.insert_str(0, "file://");
    };
}

pub fn extract_value_as_string(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return "".to_string();
    }

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            string_array.value(index).to_string()
        }
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            int_array.value(index).to_string()
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            float_array.value(index).to_string()
        }
        _ => format!("Unsupported type: {:?}", array.data_type()),
    }
}

pub fn parse_generic_url(url: &Url) -> Result<(Box<dyn ObjectStore>, Path)> {
    let (scheme, path) = ObjectStoreScheme::parse(url)
        .map_err(|err| BtrBlocksError::ObjectStore(err.to_string()))?;

    match scheme {
        ObjectStoreScheme::Local | ObjectStoreScheme::Http => {
            parse_url(url).map_err(|err| BtrBlocksError::ObjectStore(err.to_string()))
        }
        ObjectStoreScheme::AmazonS3 => Ok((
            Box::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .build()
                    .map_err(|err| BtrBlocksError::ObjectStore(err.to_string()))?,
            ),
            path,
        )),
        ObjectStoreScheme::GoogleCloudStorage => Ok((
            Box::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .build()
                    .map_err(|err| BtrBlocksError::ObjectStore(err.to_string()))?,
            ),
            path,
        )),
        _ => Err(BtrBlocksError::ObjectStore(
            "Specified URL protocol is not supported".to_string(),
        )),
    }
}
