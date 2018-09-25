////////////////////////////////////////////////////////////////////////
// Crates
////////////////////////////////////////////////////////////////////////

#[macro_use]
extern crate failure;

extern crate futures;

#[macro_use]
extern crate log;

////////////////////////////////////////////////////////////////////////
// Imports
////////////////////////////////////////////////////////////////////////

use failure::Error;

////////////////////////////////////////////////////////////////////////
// Exports
////////////////////////////////////////////////////////////////////////

pub mod communication;

pub mod subsystem;

////////////////////////////////////////////////////////////////////////

pub type ErrorResult<T> = Result<T, Error>;
