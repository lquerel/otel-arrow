// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extra standard-library contracts missing from Creusot 0.14.0.
//!
//! These are trusted assumptions about Rust primitives, not about bucket code.

use creusot_std::prelude::*;

extern_spec! {
    impl u128 {
        #[requires(rhs > 0u128)]
        #[ensures(result@ == (self@ + rhs@ - 1) / rhs@)]
        fn div_ceil(self, rhs: u128) -> u128;
    }

    impl TryFrom<u128> for u64 {
        #[ensures(match result {
            Ok(value) => input@ == value@,
            Err(_) => input@ > u64::MAX@,
        })]
        fn try_from(input: u128) -> Result<u64, std::num::TryFromIntError>;
    }
}
