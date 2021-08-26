use std::fmt::Display;
use std::str::FromStr;

///  # commands
///
///   GET <key>
///   SET <key> VAL n <value>
///   DEL <key>
///   DMP
///   EXT
///
/// # responses
///
///   VAL n <value>
///   VAL 0
///   ERR code
///   OK
///
/// # format
///
/// all commands/responses are newline terminated; VALUE may
/// contain internal newlines, and specifies value length to
/// insure that all bytes are read

#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: WrappedValue },
    Delete { key: String },
    Dump,
    Quit,
}

#[derive(Debug)]
pub enum Response {
    Value(WrappedValue),
    Err(String),
    Ok,
}

#[derive(Debug)]
pub struct WrappedValue {
    buf: Option<Vec<u8>>,
    len: usize,
}

#[derive(Debug)]
pub struct ParseError;

impl WrappedValue {
    pub fn empty() -> Self {
        Self { buf: None, len: 0 }
    }

    pub fn from_string(str: String) -> Self {
        Self {
            len: str.len(),
            buf: Some(str.into()),
        }
    }

    pub fn into_inner(self) -> String {
        if let Some(buf) = self.buf {
            String::from_utf8(buf[..self.len].to_vec()).unwrap()
        } else {
            String::new()
        }
    }
}

impl FromStr for Command {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        use Command::*;

        match &value[..3] {
            "GET" => Ok(Get {
                key: value[4..].to_string(),
            }),
            "SET" => {
                if let Some((key, value)) = value[4..].split_once(' ') {
                    Ok(Set {
                        key: key.into(),
                        value: value.parse()?,
                    })
                } else {
                    Err(ParseError)
                }
            }
            "DEL" => Ok(Delete {
                key: value[4..].to_string(),
            }),
            "DMP" => Ok(Dump),
            "EXT" => Ok(Quit),
            _ => Err(ParseError),
        }
    }
}

impl FromStr for Response {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, <Self as FromStr>::Err> {
        match &value[..2] {
            "OK" => Ok(Response::Ok),
            "ER" => Ok(Response::Err(value[4..].to_string())),
            "VA" => Ok(Response::Value(value.parse()?)),
            _ => Err(ParseError),
        }
    }
}

impl FromStr for WrappedValue {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() < 5 || &value[..4] != "VAL " {
            return Err(ParseError);
        }

        let inner = &value[4..];

        if let Some('0') = inner.chars().nth(0) {
            return Ok(WrappedValue::empty());
        }

        if let Some((length, rest)) = inner.split_once(' ') {
            return Ok(WrappedValue {
                buf: Some(rest.to_string().into()),
                len: length.parse().map_err(|_| ParseError)?,
            });
        }

        Err(ParseError)
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Command::Get { ref key } => write!(f, "GET {}", key),
            Command::Set { ref key, ref value } => write!(f, "SET {} {}", key, value),
            Command::Delete { ref key } => write!(f, "DEL {}", key),
            Command::Dump => write!(f, "DMP"),
            Command::Quit => write!(f, "EXT"),
        }
    }
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Response::Value(ref value) => write!(f, "{}", value),
            Response::Err(ref error) => write!(f, "ERR {}", error),
            Response::Ok => write!(f, "OK"),
        }
    }
}

impl Display for WrappedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            WrappedValue { len: 0, .. } => write!(f, "VAL 0"),
            WrappedValue {
                len,
                buf: Some(ref buf),
            } => {
                let str = String::from_utf8(buf[..len].to_vec()).unwrap();
                write!(f, "VAL {} {}", len, str)
            }
            _ => panic!("non-zero length with an empty buffer"),
        }
    }
}
