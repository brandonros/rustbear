use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use pty_process::OwnedWritePty;
use russh::server::{Auth, Msg, Response, Session};
use russh::*;
use russh_keys::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const USERNAME: &str = "test";
const PASSWORD: &str = "test";

#[derive(Clone)]
struct Server {
    clients: Arc<Mutex<HashMap<(usize, ChannelId), Channel<Msg>>>>,
    channel_pty_writers: Arc<Mutex<HashMap<ChannelId, OwnedWritePty>>>,
    id: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();
    let config = russh::server::Config {
        inactivity_timeout: Some(std::time::Duration::from_secs(60 * 60)),
        auth_rejection_time: std::time::Duration::from_secs(5),
        auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
        keys: vec![
            // TODO: only do this in dev
            russh_keys::key::KeyPair::generate_ed25519().unwrap(),
        ],
        ..Default::default()
    };
    let server = Server {
        clients: Arc::new(Mutex::new(HashMap::new())),
        channel_pty_writers: Arc::new(Mutex::new(HashMap::new())),
        id: 0,
    };
    log::info!("Listening on 0.0.0.0:2222");
    russh::server::run(Arc::new(config), ("0.0.0.0", 2222), server).await?;
    Ok(())
}

impl server::Server for Server {
    type Handler = Self;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Self {
        log::info!("new client");
        let s = self.clone();
        self.id += 1;
        s
    }
}

#[async_trait]
impl server::Handler for Server {
    type Error = anyhow::Error;

    async fn channel_open_session(
        self,
        channel: Channel<Msg>,
        session: Session,
    ) -> Result<(Self, bool, Session), Self::Error> {
        {
            log::info!("channel_open_session");
            let mut clients = self.clients.lock().await;
            clients.insert((self.id, channel.id()), channel);
        }
        Ok((self, true, session))
    }

    async fn env_request(
        self,
        channel_id: ChannelId,
        variable_name: &str,
        variable_value: &str,
        session: Session,
    ) -> Result<(Self, Session), Self::Error> {
        log::info!("env_request: channel_id = {channel_id} variable_name = {variable_name} variable_value = {variable_value}");
        // TODO
        Ok((self, session))
    }

    async fn exec_request(
        self,
        channel_id: ChannelId,
        command_bytes: &[u8],
        mut session: Session,
    ) -> Result<(Self, Session), Self::Error> {
        let command = String::from_utf8(command_bytes.to_vec())?;
        log::info!("exec_request: channel_id = {channel_id} command = {command}");

        // TODO: detect ssh -t vs ssh without -t (allocate pty or not)

        // create pty
        let pty = pty_process::Pty::new().unwrap();
        if let Err(e) = pty.resize(pty_process::Size::new(24, 80)) {
            log::error!("pty.resize failed: {:?}", e);
        }

        // get pts from pty
        let pts = pty.pts()?;

        // split pty into reader + writer
        let (mut pty_reader, pty_writer) = pty.into_split();

        // insert pty_reader
        self.channel_pty_writers
            .lock()
            .await
            .insert(channel_id, pty_writer);

        // pty_reader.read() -> session_handle.data()
        let session_handle = session.handle();
        tokio::spawn(async move {
            let mut buffer = vec![0; 1024];
            while let Ok(size) = pty_reader.read(&mut buffer).await {
                if size == 0 {
                    log::info!("pty_reader read 0");
                    // TODO: kill pty + command?
                    let _ = session_handle.close(channel_id).await;
                    break;
                }
                let _ = session_handle
                    .data(channel_id, CryptoVec::from_slice(&buffer[0..size]))
                    .await;
            }
        });

        // Spawn a new /bin/bash process in pty
        let mut child = pty_process::Command::new("/bin/bash")
            .arg("-c")
            .arg(command)
            .spawn(&pts)
            .map_err(anyhow::Error::new)?;

        // mark request success
        session.request_success();

        // wait for command to finish
        let _ = child.wait().await?;

        Ok((self, session))
    }

    async fn shell_request(
        self,
        channel_id: ChannelId,
        mut session: Session,
    ) -> Result<(Self, Session), Self::Error> {
        log::info!("shell_request channel_id = {channel_id}");

        // create pty
        let pty = pty_process::Pty::new().unwrap();
        if let Err(e) = pty.resize(pty_process::Size::new(24, 80)) {
            log::error!("pty.resize failed: {:?}", e);
        }

        // get pts from pty
        let pts = pty.pts()?;

        // split pty into reader + writer
        let (mut pty_reader, pty_writer) = pty.into_split();

        // insert pty_reader
        self.channel_pty_writers
            .lock()
            .await
            .insert(channel_id, pty_writer);

        // pty_reader.read() -> session_handle.data()
        let session_handle = session.handle().clone();
        tokio::spawn(async move {
            let mut buffer = vec![0; 1024];
            while let Ok(size) = pty_reader.read(&mut buffer).await {
                if size == 0 {
                    log::info!("pty_reader read 0");
                    // TODO: kill pty + command?
                    let _ = session_handle.close(channel_id).await;
                    break;
                }
                let _ = session_handle
                    .data(channel_id, CryptoVec::from_slice(&buffer[0..size]))
                    .await;
            }
        });

        // Spawn a new /bin/bash process in pty
        let program = "/bin/bash"; // TODO: get from user's shell?
        let mut child = pty_process::Command::new(program)
            .spawn(&pts)
            .map_err(anyhow::Error::new)?;

        // close session when process exits?
        let session_handle = session.handle().clone();
        tokio::spawn(async move {
            let _ = child.wait().await;
            // TODO: handle exit code?
            let _ = session_handle.close(channel_id).await;
        });

        // mark request success
        session.request_success();

        Ok((self, session))
    }

    async fn window_change_request(
        self,
        channel_id: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        session: Session,
    ) -> Result<(Self, Session), Self::Error> {
        log::info!("window_change_request channel_id = {channel_id:?} col_width = {col_width} row_height = {row_height}");
        let mut channel_pty_writers = self.channel_pty_writers.lock().await;
        if let Some(pty_writer) = channel_pty_writers.get_mut(&channel_id) {
            if let Err(e) =
                pty_writer.resize(pty_process::Size::new(row_height as u16, col_width as u16))
            {
                log::error!("pty.resize failed: {:?}", e);
            }
        }
        drop(channel_pty_writers);
        Ok((self, session))
    }

    async fn auth_publickey(
        self,
        user: &str,
        public_key: &key::PublicKey,
    ) -> Result<(Self, Auth), Self::Error> {
        log::info!("auth_publickey: user: {user} public_key: {}", public_key.public_key_base64());
        let public_key_is_valid = false; // TODO
        if public_key_is_valid {
            Ok((self, server::Auth::Accept))
        } else {
            Ok((
                self,
                Auth::Reject {
                    proceed_with_methods: Some(MethodSet::PASSWORD),
                },
            ))
        }
    }

    async fn auth_keyboard_interactive(
        self,
        user: &str,
        submethods: &str,
        _response: Option<Response<'async_trait>>,
    ) -> Result<(Self, Auth), Self::Error> {
        log::info!("auth_keyboard_interactive: user: {user} submethods: {submethods}");
        Ok((
            self,
            Auth::Reject {
                proceed_with_methods: Some(MethodSet::PUBLICKEY | MethodSet::PASSWORD),
            },
        ))
    }

    async fn auth_none(self, user: &str) -> Result<(Self, Auth), Self::Error> {
        log::info!("auth_none: user: {user}");
        Ok((
            self,
            Auth::Reject {
                proceed_with_methods: Some(MethodSet::PUBLICKEY | MethodSet::PASSWORD),
            },
        ))
    }

    async fn auth_password(self, user: &str, password: &str) -> Result<(Self, Auth), Self::Error> {
        log::info!("auth_password: credentials: {}, {}", user, password);
        let password_is_valid = user == USERNAME && password == PASSWORD;
        if password_is_valid {
            Ok((self, Auth::Accept))
        } else {
            Ok((
                self,
                Auth::Reject {
                    proceed_with_methods: None,
                },
            ))
        }
    }

    async fn channel_close(
        self,
        channel_id: ChannelId,
        session: Session,
    ) -> Result<(Self, Session), Self::Error> {
        log::info!("channel_close channel_id = {channel_id:?}");
        // TODO: cleanup
        Ok((self, session))
    }

    async fn data(
        self,
        channel_id: ChannelId,
        data: &[u8],
        session: Session,
    ) -> Result<(Self, Session), Self::Error> {
        // session_handle.data() -> pty_writer.write()
        log::info!("data channel_id = {channel_id} data = {data:02x?}");
        let mut channel_pty_writers = self.channel_pty_writers.lock().await;
        if let Some(pty_writer) = channel_pty_writers.get_mut(&channel_id) {
            log::info!("pty_writer: data = {data:02x?}");
            pty_writer
                .write_all(data)
                .await
                .map_err(anyhow::Error::new)?;
        }
        drop(channel_pty_writers);
        Ok((self, session))
    }
}
