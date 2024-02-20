// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

pub mod sync {
    #![allow(clippy::unwrap_used, clippy::missing_const_for_fn)]
    tonic::include_proto!("sync");
}

pub mod rpcdb {
    #![allow(clippy::unwrap_used, clippy::missing_const_for_fn)]
    tonic::include_proto!("rpcdb");
}

pub mod process_server {
    #![allow(clippy::unwrap_used, clippy::missing_const_for_fn)]
    tonic::include_proto!("process");
}

pub mod merkle {
    #![allow(clippy::unwrap_used, clippy::missing_const_for_fn)]
    tonic::include_proto!("merkle");

    impl From<CommitError> for String {
        fn from(e: CommitError) -> String {
            e.as_str_name().into()
        }
    }
}

pub mod service;

pub use service::Database as DatabaseService;
