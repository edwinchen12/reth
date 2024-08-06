//! Test case definitions

use crate::result::{CaseResult, Error};
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};
use futures::stream::{self, StreamExt};

/// A single test case, capable of loading a JSON description of itself and running it.
///
/// See <https://ethereum-tests.readthedocs.io/> for test specs.
pub trait Case: Debug + Sync + Sized {
    /// A description of the test.
    fn description(&self) -> String {
        "no description".to_string()
    }

    /// Load the test from the given file path.
    ///
    /// The file can be assumed to be a valid EF test case as described on <https://ethereum-tests.readthedocs.io/>.
    fn load(path: &Path) -> Result<Self, Error>;

    /// Run the test.
    async fn run(&self) -> Result<(), Error>;
}

/// A container for multiple test cases.
#[derive(Debug)]
pub struct Cases<T> {
    /// The contained test cases and the path to each test.
    pub test_cases: Vec<(PathBuf, T)>,
}

impl<T: Case> Cases<T> {
    /// Run the contained test cases.
    pub async fn run(&self) -> Vec<CaseResult> {
        stream::iter(&self.test_cases)
            .then(|(path, case)| async move { CaseResult::new(path, case, case.run().await) })
            .collect()
            .await
    }
}
