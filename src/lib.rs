use bbl_usd::{ar, cpp, ffi, tf};
use ctor::ctor;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::ffi::c_void;
use std::str::FromStr;

use iroh::{
    client::quic::{Doc, Iroh},
    net::magic_endpoint::NodeAddr,
    rpc_protocol::{BlobFormat, Hash},
    sync::NamespaceId,
    ticket::{BlobTicket, DocTicket},
};

mod util;

use util::*;

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime =
        tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    static ref IROH: Iroh = RUNTIME
        .block_on(iroh::client::quic::connect(4919))
        .expect("Failed to connect to iroh node");
}

fn string_key_to_bytes_key(string: &str) -> Vec<u8> {
    let mut bytes = string.as_bytes().to_owned();
    bytes.push(0);
    bytes
}

#[derive(Debug)]
enum NamespaceIdOrDocTicket {
    NamespaceId(NamespaceId),
    DocTicket(DocTicket),
}

impl NamespaceIdOrDocTicket {
    async fn get_doc(&self, iroh: &Iroh) -> anyhow::Result<Doc> {
        match self {
            Self::NamespaceId(namespace) => iroh
                .docs
                .open(*namespace)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Missing document: {}", namespace)),
            Self::DocTicket(ticket) => {
                if let Some(doc) = iroh.docs.open(ticket.capability.id()).await? {
                    return Ok(doc);
                }

                iroh.docs.import(ticket.clone()).await
            }
        }
    }
}

#[derive(Debug)]
enum HashOrTicket {
    Hash(Hash, String),
    Ticket(NodeAddr, Hash),
    Doc(url::Url, NamespaceIdOrDocTicket, String),
}

impl HashOrTicket {
    fn parse(raw_url: &str) -> anyhow::Result<Self> {
        let uri = url::Url::parse(raw_url)?;

        let mut components = uri
            .path_segments()
            .ok_or_else(|| anyhow::anyhow!("No path in {}", uri))?;

        Ok(match uri.domain() {
            Some("blob") => {
                let hash_and_ext = components
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing hash: {}", uri))?;
                let (hash, ext) = hash_and_ext
                    .split_once('.')
                    .ok_or_else(|| anyhow::anyhow!("Missing ext: {}", hash_and_ext))?;
                Self::Hash(iroh::rpc_protocol::Hash::from_str(hash)?, ext.to_owned())
            }
            Some("ticket") => {
                let ticket_and_ext = components
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing ticket: {}", uri))?;
                let (ticket, _ext) = ticket_and_ext
                    .split_once('.')
                    .ok_or_else(|| anyhow::anyhow!("Missing ext: {}", ticket_and_ext))?;
                let ticket = iroh::ticket::BlobTicket::from_str(ticket)?;
                let (node, hash, format) = ticket.into_parts();
                if format != BlobFormat::Raw {
                    return Err(anyhow::anyhow!("Invalid format: {:?}", format));
                }
                Self::Ticket(node, hash)
            }
            Some("doc") => {
                let namespace = components
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing namespace: {}", uri))?;
                let key = components.join("/");
                let namespace = iroh::sync::NamespaceId::from_str(namespace)?;
                Self::Doc(uri, NamespaceIdOrDocTicket::NamespaceId(namespace), key)
            }
            Some("docticket") => {
                let ticket = components
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Missing ticket: {}", uri))?;
                let key = components.join("/");
                let ticket = DocTicket::from_str(ticket)
                    .map_err(|err| anyhow::anyhow!("Failed ot parse {}: {}", uri, err))?;
                Self::Doc(uri, NamespaceIdOrDocTicket::DocTicket(ticket), key)
            }
            other => {
                return Err(anyhow::anyhow!("Invalid domain: {:?}", other));
            }
        })
    }
}
async fn handle_url(url: &str, iroh: &Iroh) -> anyhow::Result<bytes::Bytes> {
    match HashOrTicket::parse(url)? {
        HashOrTicket::Hash(hash, _ext) => iroh.blobs.read_to_bytes(hash).await,
        HashOrTicket::Ticket(peer, hash) => {
            if let Ok(bytes) = iroh.blobs.read_to_bytes(hash).await {
                return Ok(bytes);
            }

            let download_stream = iroh
                .blobs
                .download(iroh::rpc_protocol::BlobDownloadRequest {
                    hash,
                    peer,
                    format: BlobFormat::Raw,
                    tag: iroh::rpc_protocol::SetTagOption::Auto,
                    out: iroh::rpc_protocol::DownloadLocation::Internal,
                })
                .await?;

            finish_download(download_stream).await?;

            iroh.blobs.read_to_bytes(hash).await
        }
        HashOrTicket::Doc(_, namespace_or_ticket, key) => {
            let doc = namespace_or_ticket.get_doc(iroh).await.map_err(|err| {
                err.context(format!("Failed to get doc {:?}", &namespace_or_ticket))
            })?;
            get_key_in_doc(&doc, iroh, &string_key_to_bytes_key(&key))
                .await
                .map_err(|err| err.context(format!("Failed to get key: {:?}", key)))
        }
    }
}

extern "C" fn open_asset(
    path: *const ffi::ar_ResolvedPath_t,
    output: *mut *mut ffi::ar_AssetSharedPtr_t,
) {
    let string = ar::ResolvedPathRef::from_raw(path).get_path_string();
    let future = |iroh| async move {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            handle_url(string.as_str(), iroh),
        )
        .await
    };
    match RUNTIME.block_on(future(&IROH)) {
        Ok(Ok(bytes)) => unsafe {
            ffi::ar_asset_from_bytes(bytes.as_ptr() as _, bytes.len(), output);
        },
        Ok(Err(error)) => {
            println!("{:#}", error);
        }
        Err(timedout) => {
            println!("Timed out: {}", timedout);
        }
    }
}

extern "C" fn resolve(string: *const ffi::std_String_t, output: *mut *mut ffi::ar_ResolvedPath_t) {
    let string = cpp::StringRef::from_ptr(string);
    let resolved_path = ar::ResolvedPath::new(&string);
    unsafe { *output = resolved_path.ptr() as _ };
    std::mem::forget(resolved_path);
}

extern "C" fn create_identifier(
    path: *const ffi::std_String_t,
    anchor: *const ffi::ar_ResolvedPath_t,
    output: *mut *mut ffi::std_String_t,
) {
    let anchor = ar::ResolvedPathRef::from_raw(anchor).get_path_string();
    let anchor = anchor.as_str();
    let path = cpp::StringRef::from_ptr(path);

    let try_create_identifier = || {
        match (
            HashOrTicket::parse(path.as_str()),
            HashOrTicket::parse(anchor),
        ) {
            // If the path is just a hash but the anchor is a ticket, construct a new ticket using
            // the peer from the anchor ticket.
            (Ok(HashOrTicket::Hash(hash, ext)), Ok(HashOrTicket::Ticket(node, ..))) => {
                let ticket = BlobTicket::new(node, hash, iroh::rpc_protocol::BlobFormat::Raw)?;

                let new_url = format!("iroh://ticket/{}.{}", ticket, ext);
                let new_url = cpp::String::new(&new_url);

                unsafe {
                    *output = new_url.ptr() as _;
                }
                std::mem::forget(new_url);
            }
            (Err(_), Ok(HashOrTicket::Doc(url, ..))) => {
                let url = url.join(path.as_str())?;

                let new_url = url.to_string();
                let new_url = cpp::String::new(&new_url);

                unsafe {
                    *output = new_url.ptr() as _;
                }
                std::mem::forget(new_url);
            }
            _ => unsafe {
                *output = path.ptr() as *mut ffi::std_String_t;
            },
        }

        Ok::<_, anyhow::Error>(())
    };

    if let Err(error) = try_create_identifier() {
        println!("{}", error);
    }
}

extern "C" fn get_modification_timestamp(
    path: *const ffi::std_String_t,
    _resolved: *const ffi::ar_ResolvedPath_t,
    timestamp: *mut *mut ffi::ar_Timestamp_t,
) {
    let path = cpp::StringRef::from_ptr(path);

    let try_get_modifcation_timestamp = |iroh| async move {
        match HashOrTicket::parse(path.as_str()) {
            // If the path doesn't point to a document, just set the timestamp to 0.
            Ok(HashOrTicket::Hash(..) | HashOrTicket::Ticket(..)) | Err(_) => {
                let ar_timestamp = ar::Timestamp::from_time(0.0);
                unsafe {
                    *timestamp = ar_timestamp.ptr() as _;
                };
                std::mem::forget(ar_timestamp);
                return Ok(());
            }
            // Otherwise try to load a document and get the timestamp of the entry matching the key.
            Ok(HashOrTicket::Doc(_, namespace_or_ticket, key)) => {
                let doc = namespace_or_ticket.get_doc(iroh).await?;
                let entry = get_entry_in_doc(&doc, &string_key_to_bytes_key(&key)).await?;
                let ar_timestamp = ar::Timestamp::from_time(entry.timestamp() as f64);
                unsafe {
                    *timestamp = ar_timestamp.ptr() as _;
                };
                std::mem::forget(ar_timestamp);
            }
        };

        Ok::<_, anyhow::Error>(())
    };

    if let Err(error) = RUNTIME.block_on(try_get_modifcation_timestamp(&IROH)) {
        println!("{}", error);
    }
}

struct WritableAsset {
    path: String,
    bytes: Vec<u8>,
}

extern "C" fn open_writable_asset(
    path: *const ffi::ar_ResolvedPath_t,
    mode: ffi::ar_ResolvedWriteMode,
) -> *mut c_void {
    assert_eq!(mode, ffi::ar_ResolvedWriteMode_ar_ResolvedWriteMode_Replace);
    Box::into_raw(Box::new(WritableAsset {
        path: ar::ResolvedPathRef::from_raw(path)
            .get_path_string()
            .as_str()
            .to_owned(),
        bytes: Vec::new(),
    })) as _
}

extern "C" fn close_writable_asset(context: *mut c_void) -> bool {
    let asset = unsafe { &*(context as *mut WritableAsset) };

    let try_fn = |iroh| async move {
        let author_id = std::env::var("IROH_AUTHOR_ID")?;
        let author_id = iroh::sync::AuthorId::from_str(&author_id)?;

        match HashOrTicket::parse(&asset.path) {
            Ok(HashOrTicket::Doc(_, namespace_or_ticket, key)) => {
                let doc = namespace_or_ticket.get_doc(iroh).await?;
                let key = string_key_to_bytes_key(&key);
                doc.set_bytes(
                    author_id,
                    bytes::Bytes::copy_from_slice(&key),
                    &*asset.bytes,
                )
                .await?;
                Ok(())
            }
            other => {
                return Err(anyhow::anyhow!(
                    "close_writable_asset parsing error: {:?}",
                    other
                ))
            }
        }
    };

    if let Err(err) = RUNTIME.block_on(try_fn(&IROH)) {
        println!("{}", err);
        false
    } else {
        true
    }
}

extern "C" fn write_writable_asset(
    context: *mut c_void,
    src: *const c_void,
    count: usize,
    offset: usize,
) -> usize {
    let asset = unsafe { &mut *(context as *mut WritableAsset) };

    let src = unsafe { std::slice::from_raw_parts(src as *const u8, count) };

    if count + offset > asset.bytes.len() {
        asset.bytes.resize(count + offset, 0);
    }

    asset.bytes[offset..offset + count].copy_from_slice(src);

    count
}

#[ctor]
fn ctor() {
    let ty = tf::Type::declare("IrohResolver");
    ty.set_factory(
        Some(create_identifier),
        Some(create_identifier),
        Some(open_asset),
        Some(resolve),
        Some(resolve),
        None,
        Some(get_modification_timestamp),
        Some(close_writable_asset),
        Some(open_writable_asset),
        Some(write_writable_asset),
    );
}
