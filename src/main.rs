#![allow(dead_code)]
use std::fmt::Display;
use std::io;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use lazy_static::lazy_static;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

lazy_static! {
    static ref STORE: DashMap<String, String> = DashMap::new();
}

pub struct Connection {
    socket: TcpStream,
    store: DashMap<String, String>,
    buffer: [u8; Self::BUFLEN],
    buflen: usize,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = ("127.0.0.1", 6791);
    let listener = TcpListener::bind(addr).await?;
    println!("Started  conn on: {addr:?}");
    let store = DashMap::new();
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("Connection on {addr:?}");
                let store = store.clone();
                tokio::spawn(async move {
                    let conn = Connection::new(socket, store.clone());
                    if let Err(e) = conn.run().await {
                        println!("Error: {e:?}");
                    }
                });
            }
            Err(e) => {
                println!("Error: {e:?}");
            }
        }
    }
}

impl Connection {
    const BUFLEN: usize = 1024;

    pub fn new(socket: TcpStream, store: DashMap<String, String>) -> Self {
        Self {
            socket,
            store,
            buffer: [0; Self::BUFLEN],
            buflen: 0,
        }
    }

    pub async fn run(mut self) -> Result<(), MyError> {
        loop {
            match self.read_command().await {
                Ok(cmd) => self.reply(self.handle_command(cmd)).await?,
                Err(e) => match e {
                    MyError::Io(_) => todo!(),
                    MyError::InvalidCommand => todo!(),
                    MyError::NotEnoughArgs => todo!(),
                    MyError::NoCommand => todo!(),
                    MyError::MessageTooLong => todo!(),
                    MyError::Disconnected => todo!(),
                    MyError::ConnectClosed => todo!(),
                    MyError::NonUtf8 => todo!(),
                },
            }
            match self.socket.read(&mut self.buffer).await {
                Ok(0) => break,
                Ok(n) => {
                    // handle the input
                    let s = std::str::from_utf8(&mut self.buffer).unwrap();
                    println!("Received: `{s}` of len({n})");
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn reply(&mut self, resp: Response) -> Result<(), MyError> {
        let resp = resp.to_string();
    }

    async fn read_command(&mut self) -> Result<Command, MyError> {
        // No end - try read the end;
        if self.buffer_crlf().is_none() {
            // Try read
            let bytes_read = loop {
                match self.socket.read(&mut self.buffer[self.buflen..]).await {
                    Ok(0) => return Err(MyError::ConnectClosed),
                    Ok(n) => break n,
                    Err(e) => match e.kind() {
                        std::io::ErrorKind::Interrupted => continue,
                        _ => return Err(MyError::Disconnected),
                    },
                }
            };
            self.buflen += bytes_read;
        }
        let end_index = self.buffer_crlf().ok_or_else(|| {
            self.clear_buffer();
            MyError::MessageTooLong
        })?;

        let msg = String::from_utf8(self.buffer[..end_index].to_vec()).map_err(|_| {
            self.clear_buffer();
            MyError::NonUtf8
        })?;

        self.cycle_buffer();

        Ok(Command::try_from(msg.as_str())?)
    }

    fn cycle_buffer(&mut self) {
        let start = match self.buffer_crlf() {
            Some(index) => index + 2, // Account for '\r\n'
            None => return,
        };
        self.buffer.copy_within(start..self.buflen, 0);
        self.buflen -= start;
    }

    fn clear_buffer(&mut self) {
        // self.buffer = [0; 1024];
        self.buflen = 0;
    }

    fn handle_command(&mut self, cmd: Command) -> Response {
        let own = |op: Option<Ref<'_, _, String, _>>| op.map(|v| v.to_string());

        match cmd {
            Command::Ping => Response::Pong,
            Command::Echo(v) => Response::Echo(v),
            Command::Get(key) => Response::Get(own(self.store.get(&key))),
            Command::Set(key, val) => Response::Set(self.store.insert(key, val)),
            Command::Del(val) => Response::Del(self.store.remove(&val).map(|(_, v)| v)),
        }
    }

    fn buffer_crlf(&self) -> Option<usize> {
        self.buffer[..self.buflen]
            .windows(2)
            .enumerate()
            .find(|(_, bytes)| (bytes[0], bytes[1]) == (b'\r', b'\n'))
            .map(|(index, _)| index)
    }
}

enum Response {
    Pong,
    Echo(String),
    Get(Option<String>),
    Set(Option<String>),
    Del(Option<String>),
}

enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set(String, String),
    Del(String),
}

impl TryFrom<&str> for Command {
    type Error = MyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut split = value.split(" ");

        let cmd = split.next();
        let mut next_arg = || match split.next() {
            Some(v) => Ok(v.to_string()),
            None => Err(Self::Error::NotEnoughArgs),
        };

        match cmd {
            Some("PING") => Ok(Self::Ping),
            Some("ECHO") => Ok(Self::Echo(next_arg()?)),
            Some("GET") => Ok(Self::Get(next_arg()?)),
            Some("SET") => Ok(Self::Set(next_arg()?, next_arg()?)),
            Some("DEL") => Ok(Self::Del(next_arg()?)),
            Some(_) => Err(Self::Error::InvalidCommand),
            None => Err(Self::Error::NoCommand),
        }
    }
}

pub enum MyError {
    Io(io::Error),
    InvalidCommand,
    NotEnoughArgs,
    NoCommand,
    MessageTooLong,
    Disconnected,
    ConnectClosed,
    NonUtf8,
}

/// TODO(imagine-hussain): handle escape characters
impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Pong => write!(f, "PONG")?,
            Response::Echo(s) => write!(f, "{s}")?,
            Response::Get(v) => {
                write!(f, "GET")?;
                write_op(f, v.as_ref())?;
            }
            Response::Set(v) => {
                write!(f, "SET")?;
                write_op(f, v.as_ref())?;
            }
            Response::Del(v) => {
                write!(f, "DEL")?;
                write_op(f, v.as_ref())?;
            }
        };
        write!(f, "\r\n")
    }
}

fn write_op(f: &mut std::fmt::Formatter<'_>, op: Option<impl Display>) -> std::fmt::Result {
    match op {
        Some(v) => write!(f, "{}", v),
        None => write!(f, "(nil)"),
    }
}
