// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use chrono::Datelike;

use crate::error::Error::RowConvertError;
use crate::error::Result;
use arrow::array::{ArrayBuilder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, BooleanBuilder, StringBuilder, BinaryBuilder, Date32Builder, TimestampNanosecondBuilder};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use parse_display::Display;
use ref_cast::RefCast;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

#[allow(dead_code)]
const THIRTY_YEARS_MICROSECONDS: i64 = 946_684_800_000_000;

pub const UNIX_EPOCH_DAYS: i32 = 719_163;

#[derive(Debug, Clone, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Datum<'a> {
    #[display("null")]
    Null,
    #[display("{0}")]
    Bool(bool),
    #[display("{0}")]
    Int16(i16),
    #[display("{0}")]
    Int32(i32),
    #[display("{0}")]
    Int64(i64),
    #[display("{0}")]
    Float32(F32), 
    #[display("{0}")]
    Float64(F64),
    #[display("'{0}'")]
    String(&'a str),
    #[display("{0}")]
    Blob(Blob),
    #[display("{0}")]
    Decimal(Decimal),
    #[display("{0}")]
    Date(Date),
    #[display("{0}")]
    Timestamp(Timestamp),
    #[display("{0}")]
    TimestampTz(TimestampLtz),
}

impl Datum<'_> {
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(s) => s,
            _ => panic!("not a string: {self:?}"),
        }
    }
}

// ----------- implement from
impl<'a> From<i32> for Datum<'a> {
    #[inline]
    fn from(i: i32) -> Datum<'a> {
        Datum::Int32(i)
    }
}

impl<'a> From<&'a str> for Datum<'a> {
    #[inline]
    fn from(s: &'a str) -> Datum<'a> {
        Datum::String(s)
    }
}

impl From<Option<&()>> for Datum<'_> {
    fn from(_: Option<&()>) -> Self {
        Self::Null
    }
}

impl<'a> From<f32> for Datum<'a> {
    #[inline]
    fn from(f: f32) -> Datum<'a> {
        Datum::Float32(F32::from(f))
    }
}

impl<'a> From<f64> for Datum<'a> {
    #[inline]
    fn from(f: f64) -> Datum<'a> {
        Datum::Float64(F64::from(f))
    }
}

impl TryFrom<&Datum<'_>> for i32 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int32(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl<'a> TryFrom<&Datum<'a>> for &'a str {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum<'a>) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::String(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

pub trait ToArrow {
    fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()>;
}

impl Datum<'_> {
    pub fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        macro_rules! append_by_type {
            ($builder_type:ty, append_null) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    b.append_null();
                    return Ok(());
                }
            };
            ($builder_type:ty, append_value, $value:expr) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    b.append_value($value);
                    return Ok(());
                }
            };
        }
        
        match self {
            Datum::Null => {
                append_by_type!(BooleanBuilder, append_null);
                append_by_type!(Int16Builder, append_null);
                append_by_type!(Int32Builder, append_null);
                append_by_type!(Int64Builder, append_null);
                append_by_type!(Float32Builder, append_null);
                append_by_type!(Float64Builder, append_null);
                append_by_type!(StringBuilder, append_null);
                append_by_type!(BinaryBuilder, append_null);
            }
            Datum::Bool(v) => append_by_type!(BooleanBuilder, append_value, *v),
            Datum::Int16(v) => append_by_type!(Int16Builder, append_value, *v),
            Datum::Int32(v) => append_by_type!(Int32Builder, append_value, *v),
            Datum::Int64(v) => append_by_type!(Int64Builder, append_value, *v),
            Datum::Float32(v) => append_by_type!(Float32Builder, append_value, v.into_inner()),
            Datum::Float64(v) => append_by_type!(Float64Builder, append_value, v.into_inner()),
            Datum::String(v) => append_by_type!(StringBuilder, append_value, *v),
            Datum::Blob(v) => append_by_type!(BinaryBuilder, append_value, v.as_ref()),
            Datum::Decimal(_) | Datum::Date(_) | Datum::Timestamp(_) | Datum::TimestampTz(_) => {
                return Err(RowConvertError(format!(
                    "Type {:?} is not yet supported for Arrow conversion", 
                    std::mem::discriminant(self)
                )));
            }
        }

        Err(RowConvertError(format!(
            "Cannot append {:?} to builder of type {}",
            self,
            std::any::type_name_of_val(builder)
        )))
    }
}

macro_rules! impl_to_arrow {
    ($ty:ty, $variant:ident) => {
        impl ToArrow for $ty {
            fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$variant>() {
                    b.append_value(*self);
                    Ok(())
                } else {
                    Err(RowConvertError(format!(
                        "Cannot cast {} to {} builder",
                        stringify!($ty),
                        stringify!($variant)
                    )))
                }
            }
        }
    };
}

impl_to_arrow!(i8, Int8Builder);
impl_to_arrow!(i16, Int16Builder);
impl_to_arrow!(i32, Int32Builder);
impl_to_arrow!(f32, Float32Builder);
impl_to_arrow!(f64, Float64Builder);
impl_to_arrow!(&str, StringBuilder);

#[allow(dead_code)]
pub type F32 = OrderedFloat<f32>;
pub type F64 = OrderedFloat<f64>;
#[allow(dead_code)]
pub type Str = Box<str>;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, Default)]
pub struct Blob(Box<[u8]>);

impl Deref for Blob {
    type Target = BlobRef;

    fn deref(&self) -> &Self::Target {
        BlobRef::new(&self.0)
    }
}

impl BlobRef {
    pub fn new(bytes: &[u8]) -> &Self {
        // SAFETY: `&BlobRef` and `&[u8]` have the same layout.
        BlobRef::ref_cast(bytes)
    }
}

/// A slice of a blob.
#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord, RefCast, Hash)]
pub struct BlobRef([u8]);

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl fmt::Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl AsRef<[u8]> for BlobRef {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for BlobRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Date(i32);

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Timestamp(i64);

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct TimestampLtz(i64);

impl From<Vec<u8>> for Blob {
    fn from(vec: Vec<u8>) -> Self {
        Blob(vec.into())
    }
}

impl Date {
    pub const fn new(inner: i32) -> Self {
        Date(inner)
    }

    /// Get the inner value of date type
    pub fn get_inner(&self) -> i32 {
        self.0
    }

    pub fn year(&self) -> i32 {
        let date = NaiveDate::from_num_days_from_ce_opt(self.0 + UNIX_EPOCH_DAYS).unwrap();
        date.year()
    }
    pub fn month(&self) -> i32 {
        let date = NaiveDate::from_num_days_from_ce_opt(self.0 + UNIX_EPOCH_DAYS).unwrap();
        date.month() as i32
    }
    pub fn day(&self) -> i32 {
        let date = NaiveDate::from_num_days_from_ce_opt(self.0 + UNIX_EPOCH_DAYS).unwrap();
        date.day() as i32
    }
}
