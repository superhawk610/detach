use clap::{AppSettings, Clap};
use daemonize::{Daemonize, DaemonizeError};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

const PID: &'static str = "/tmp/detach.pid";
const SOCKET: &'static str = "/tmp/detach.sock";
const STDOUT: &'static str = "/tmp/detach.out";
const STDERR: &'static str = "/tmp/detach.err";

#[derive(Clap, Debug)]
#[clap(version = "0.1.0", author = "Aaron R. <superhawk610@gmail.com>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(subcommand)]
    action: Action,
}

#[derive(Clap, Debug)]
enum Action {
    #[clap(about = "Spawn a worker in the background.")]
    Worker,
    Get(GetAction),
    Set(SetAction),
    #[clap(about = "Dump the background worker's state.")]
    Dump,
    #[clap(about = "Close the background worker (if one is open).")]
    Quit,
}

#[derive(Clap, Debug)]
#[clap(about = "Retrieve the value for a key (if any).")]
struct GetAction {
    #[clap(index = 1)]
    key: String,
}

#[derive(Clap, Debug)]
#[clap(about = "Set the value for a key (overwriting any already set).")]
struct SetAction {
    #[clap(index = 1)]
    key: String,
    #[clap(index = 2)]
    value: String,
}

fn main() {
    match handle_command(Opts::parse()) {
        Ok(_) => (),
        Err(error) => eprintln!("{}", error),
    }
}

fn handle_command(opts: Opts) -> io::Result<()> {
    match opts.action {
        Action::Worker => worker_command(),
        Action::Get(GetAction { key }) => get_command(key),
        Action::Set(SetAction { key, value }) => set_command(key, value),
        Action::Dump => dump_command(),
        Action::Quit => quit_command(),
    }
}

struct Stream {
    inner: UnixStream,
    path: PathBuf,
}

impl Stream {
    fn connect<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = PathBuf::from(path.as_ref());
        Ok(Self {
            inner: UnixStream::connect(path)?,
            path: path_buf,
        })
    }
}

impl std::ops::Drop for Stream {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.as_path()).unwrap();
    }
}

impl std::ops::Deref for Stream {
    type Target = UnixStream;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Stream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

fn connect() -> io::Result<Stream> {
    Stream::connect(SOCKET)
}

fn send(stream: &mut UnixStream, command: SocketCommand) -> io::Result<()> {
    stream.write_all(format!("{}\n", command).as_bytes())
}

fn recv(stream: &mut UnixStream) -> io::Result<SocketResponse> {
    // most commands (other than SET) should fit in 16 bytes
    let mut res = String::with_capacity(16);
    let mut buf_reader = io::BufReader::new(stream);
    buf_reader.read_line(&mut res)?;
    res.pop(); // trim trailing newline

    parse_response(res)
}

fn command(cmd: SocketCommand) -> io::Result<()> {
    let mut stream = connect()?;
    send(&mut stream, cmd)?;

    let res = recv(&mut stream)?;
    println!("{}", res);

    Ok(())
}

fn get_command(key: String) -> io::Result<()> {
    command(SocketCommand::Get { key })
}

fn set_command(key: String, value: String) -> io::Result<()> {
    command(SocketCommand::Set { key, value })
}

fn dump_command() -> io::Result<()> {
    command(SocketCommand::Dump)
}

fn quit_command() -> io::Result<()> {
    command(SocketCommand::Quit)
}

fn worker_command() -> io::Result<()> {
    let stdout = File::create(STDOUT).unwrap();
    let stderr = File::create(STDERR).unwrap();

    let daemonize = Daemonize::new()
        .pid_file(PID)
        .working_directory("/tmp")
        .stdout(stdout)
        .stderr(stderr)
        .exit_action(|| println!("started background worker"));

    match daemonize.start() {
        Ok(_) => start_socket()?,
        Err(DaemonizeError::LockPidfile(_)) => eprintln!("server already running"),
        Err(e) => eprintln!("oh no! {}", e),
    }

    Ok(())
}

#[derive(Debug, Default)]
struct AppState {
    count: u8,
    db: HashMap<String, String>,
    should_terminate: bool,
}

fn start_socket() -> io::Result<()> {
    println!("I'm a worker!");

    let listener = UnixListener::bind(SOCKET).expect("unable to bind to socket");
    let mut state = AppState::default();

    loop {
        match listener.accept() {
            Ok((socket, addr)) => {
                eprintln!("got connection {:?}", addr);
                accept_connection(socket, &mut state)?;
            }
            Err(e) => eprintln!("unable to accept connection {:?}", e),
        }

        if state.should_terminate {
            break;
        }
    }

    Ok(())
}

// # commands
//
//   GET <key>
//   SET <key> <value>
//   DUMP
//   QUIT
//
// # responses
//
//   VALUE <value>
//   OK
//

fn accept_connection(mut socket: UnixStream, state: &mut AppState) -> io::Result<()> {
    let mut req = String::with_capacity(16);
    let mut buf_reader = io::BufReader::new(&mut socket);
    buf_reader.read_line(&mut req)?;
    req.pop(); // remove trailing newline

    let res = match parse_command(req)? {
        SocketCommand::Get { key } => SocketResponse::Value(state.db.get(&key).cloned()),
        SocketCommand::Set { key, value } => {
            state.db.insert(key, value);
            SocketResponse::Ok
        }
        SocketCommand::Dump => SocketResponse::Value(Some(format!("{:?}", state.db))),
        SocketCommand::Quit => {
            state.should_terminate = true;
            SocketResponse::Ok
        }
    };

    socket.write_all(format!("{}\n", res).as_bytes())?;

    state.count += 1;
    eprintln!("state: {:?}", state);

    Ok(())
}

#[derive(Debug)]
enum SocketCommand {
    Get { key: String },
    Set { key: String, value: String },
    Dump,
    Quit,
}

#[derive(Debug)]
enum SocketResponse {
    Value(Option<String>),
    Err(String),
    Ok,
}

// FIXME: implement TryInto
fn parse_command(mut res: String) -> io::Result<SocketCommand> {
    match &res[..4] {
        "GET " => Ok(SocketCommand::Get {
            key: res.split_off(4),
        }),
        "SET " => {
            let key_value = res.split_off(4);
            if let Some((key, value)) = key_value.split_once(' ') {
                Ok(SocketCommand::Set {
                    key: key.into(),
                    value: value.into(),
                })
            } else {
                Err(io::Error::new(io::ErrorKind::Other, "not enough args"))
            }
        }
        "DUMP" => Ok(SocketCommand::Dump),
        "QUIT" => Ok(SocketCommand::Quit),
        _ => Err(io::Error::new(io::ErrorKind::Other, "unrecognized command")),
    }
}

// FIXME: implement TryInto
fn parse_response(mut res: String) -> io::Result<SocketResponse> {
    match &res[..2] {
        "OK" => Ok(SocketResponse::Ok),
        "ER" => Ok(SocketResponse::Err(res.split_off(4))),
        "VA" => Ok(SocketResponse::Value(Some(res.split_off(6)))),
        _ => Err(io::Error::new(io::ErrorKind::Other, "unrecognized command")),
    }
}

impl std::fmt::Display for SocketCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            SocketCommand::Get { ref key } => write!(f, "GET {}", key),
            SocketCommand::Set { ref key, ref value } => write!(f, "SET {} {}", key, value),
            SocketCommand::Dump => write!(f, "DUMP"),
            SocketCommand::Quit => write!(f, "QUIT"),
        }
    }
}

impl std::fmt::Display for SocketResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            SocketResponse::Value(Some(ref value)) => write!(f, "VALUE {}", value),
            SocketResponse::Value(None) => write!(f, "VALUE <null>"),
            SocketResponse::Err(ref error) => write!(f, "ERR {}", error),
            SocketResponse::Ok => write!(f, "OK"),
        }
    }
}
