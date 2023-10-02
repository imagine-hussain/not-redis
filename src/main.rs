#![allow(dead_code)]
use std::fmt::Display;
use std::io;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct Connection {
    socket: TcpStream,
    store: DashMap<String, String>,
    buffer: [u8; Self::BUFLEN],
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
        }
    }

    pub async fn run(mut self) -> Result<(), MyError> {
        loop {
            println!("in run loop");
            match self.read_command().await {
                Ok(cmd) => {
                    let resp = self.handle_command(cmd);
                    self.reply(resp).await?;
                }
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
                Err(e) => return Err(MyError::from(e)),
            }
        }
        Ok(())
    }

    async fn reply(&mut self, resp: Response) -> Result<(), MyError> {
        let resp = resp.to_bytes();
        self.socket.write_u32(resp.len() as u32).await?;
        self.socket.write_all(resp.as_bytes()).await?;
        self.socket.flush().await?;
        Ok(())
    }

    async fn read_command(&mut self) -> Result<Command, MyError> {
        let msg_size = self.socket.read_u32().await? as usize;
        if msg_size > Self::BUFLEN {
            return Err(MyError::MessageTooLong);
        }

        let buf = &mut self.buffer[..msg_size];
        self.socket.read_exact(buf).await?;

        let msg = dbg!(String::from_utf8(buf.to_vec()).map_err(|_| MyError::NonUtf8)?);

        Ok(Command::try_from(msg.as_str())?)
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

impl Response {
    fn to_bytes(&self) -> String {
        match self {
            Response::Pong => String::from("PONG"),
            Response::Echo(s) => format!("ECHO {}", s),
            Response::Get(v) => match v {
                Some(v) => format!("GET {}", v),
                None => format!("GET (nil)"),
            },
            Response::Set(v) => match v {
                Some(v) => format!("SET {}", v),
                None => format!("SET (nil)"),
            },
            Response::Del(v) => match v {
                Some(v) => format!("DEL {}", v),
                None => format!("DEL (nil)"),
            },
        }
    }
}

impl TryFrom<&str> for Command {
    type Error = MyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        println!("parsing: `{value}`");
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

#[derive(Debug)]
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

fn write_op(f: &mut std::fmt::Formatter<'_>, op: Option<impl Display>) -> std::fmt::Result {
    match op {
        Some(v) => write!(f, "{}", v),
        None => write!(f, "(nil)"),
    }
}

fn option_string_len(s: Option<&impl AsRef<str>>) -> usize {
    match s {
        None => "(nil)".len(),
        Some(s) => s.as_ref().len(),
    }
}

impl From<io::Error> for MyError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
