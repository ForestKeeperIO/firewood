// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

pub mod io {
    pub mod prometheus {
        pub mod client {
            #![allow(clippy::unwrap_used)]
            #![allow(clippy::missing_const_for_fn)]
            tonic::include_proto!("io.prometheus.client");
        }
    }
}

pub mod sync {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::missing_const_for_fn)]
    tonic::include_proto!("sync");
}

pub mod rpcdb {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::missing_const_for_fn)]
    tonic::include_proto!("rpcdb");
}

pub mod process_server {
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::missing_const_for_fn)]
    tonic::include_proto!("process");
}

pub mod service;

pub use service::Database as DatabaseService;
