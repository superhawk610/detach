use clap::{AppSettings, Clap};
use daemonize::{Daemonize, DaemonizeError};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

use detach::serialize;

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
    Delete(DeleteAction),
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

#[derive(Clap, Debug)]
#[clap(about = "Delete the value for a key (if any).")]
struct DeleteAction {
    #[clap(index = 1)]
    key: String,
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
        Action::Delete(DeleteAction { key }) => delete_command(key),
        Action::Dump => dump_command(),
        Action::Quit => quit_command(),
    }
}

struct Socket {
    inner: UnixListener,
    path: PathBuf,
}

impl Socket {
    fn bind<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = PathBuf::from(path.as_ref());
        Ok(Self {
            inner: UnixListener::bind(path)?,
            path: path_buf,
        })
    }
}

impl std::ops::Drop for Socket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.as_path()).unwrap();
    }
}

impl std::ops::Deref for Socket {
    type Target = UnixListener;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Socket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

fn connect() -> io::Result<UnixStream> {
    UnixStream::connect(SOCKET)
}

fn send(stream: &mut UnixStream, command: serialize::Command) -> io::Result<()> {
    stream.write_all(format!("{}\n", command).as_bytes())
}

fn recv(stream: &mut UnixStream) -> io::Result<serialize::Response> {
    // most commands (other than SET) should fit in 16 bytes
    let mut res = String::with_capacity(16);
    let mut buf_reader = io::BufReader::new(stream);
    buf_reader.read_line(&mut res)?;
    res.pop(); // trim trailing newline

    res.parse()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "parse error"))
}

fn command(cmd: serialize::Command) -> io::Result<()> {
    let mut stream = connect()?;
    send(&mut stream, cmd)?;

    let res = recv(&mut stream)?;
    println!("{}", res);

    Ok(())
}

fn get_command(key: String) -> io::Result<()> {
    command(serialize::Command::Get { key })
}

fn set_command(key: String, value: String) -> io::Result<()> {
    command(serialize::Command::Set {
        key,
        value: serialize::WrappedValue::from_string(value),
    })
}

fn delete_command(key: String) -> io::Result<()> {
    command(serialize::Command::Delete { key })
}

fn dump_command() -> io::Result<()> {
    command(serialize::Command::Dump)
}

fn quit_command() -> io::Result<()> {
    command(serialize::Command::Quit)
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

    let listener = Socket::bind(SOCKET).expect("unable to bind to socket");
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

fn accept_connection(mut socket: UnixStream, state: &mut AppState) -> io::Result<()> {
    let mut req = String::with_capacity(16);
    let mut buf_reader = io::BufReader::new(&mut socket);
    buf_reader.read_line(&mut req)?;
    req.pop(); // remove trailing newline

    let res = match req
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "parse error"))?
    {
        serialize::Command::Get { key } => {
            serialize::Response::Value(serialize::WrappedValue::from_string(
                state.db.get(&key).cloned().unwrap_or_else(String::new),
            ))
        }
        serialize::Command::Set { key, value } => {
            state.db.insert(key, value.into_inner());
            serialize::Response::Ok
        }
        serialize::Command::Delete { key } => {
            state.db.remove(&key);
            serialize::Response::Ok
        }
        serialize::Command::Dump => serialize::Response::Value(
            serialize::WrappedValue::from_string(format!("{:?}", state.db)),
        ),
        serialize::Command::Quit => {
            state.should_terminate = true;
            serialize::Response::Ok
        }
    };

    socket.write_all(format!("{}\n", res).as_bytes())?;

    state.count += 1;
    eprintln!("state: {:?}", state);

    Ok(())
}
