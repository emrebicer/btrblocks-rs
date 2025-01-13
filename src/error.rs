use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum BtrBlocksError {
    #[error("this type is not supported: `{0}`")]
    UnexpectedType(String),
    #[error("something went wrong with btr metadata: `{0}`")]
    Metadata(String),
    #[error("something went wrong with path: `{0}`")]
    Path(String),
    #[error("an error occured on the BtrBlocks c++ library wrapper: `{0}`")]
    BtrBlocksLibWrapper(String),
    #[error("Failed to parse given URL: `{0}`")]
    Url(String),
    #[error("a custom error occured: `{0}`")]
    Custom(String),
}
