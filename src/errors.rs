use foundationdb::{FdbError, TransactionCommitError};
use pgrx::{ereport, PgSqlErrorCode};

// Define a custom trait to extend FdbError
pub trait FdbErrorExt<T> {
    fn unwrap_or_pg_error(self) -> T;
}

impl<T> FdbErrorExt<T> for Result<T, FdbError> {
    fn unwrap_or_pg_error(self) -> T {
        match self {
            Ok(value) => value,
            Err(err) => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_SYSTEM_ERROR,
                    &format!("FDB error {}: {}", err.code(), err)
                );
            }
        }
    }
}

impl<T> FdbErrorExt<T> for Result<T, TransactionCommitError> {
    fn unwrap_or_pg_error(self) -> T {
        match self {
            Ok(value) => value,
            Err(err) => {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_SYSTEM_ERROR,
                    &format!("FDB error {}: {}", err.code(), err)
                );
            }
        }
    }
}
