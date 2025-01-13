use url::Url;

use crate::{error::BtrBlocksError, Result};

pub fn string_to_btr_url(btr_url: &mut String) -> Result<Url> {
    // Since btr compression ends up in a folder structure, the ending must be a slash
    if !btr_url.ends_with("/") {
        btr_url.push('/');
    };

    // The Url expects a protocol, if there is no protocol found in the passed in string,
    // assume this is a local file
    if !btr_url.starts_with("file://")
        && !btr_url.starts_with("s3://")
        && !btr_url.starts_with("gs://")
        && !btr_url.starts_with("gcs://")
    {
        btr_url.insert_str(0, "file://");
    };

    Url::parse(btr_url).map_err(|err| BtrBlocksError::Url(err.to_string()))
}
