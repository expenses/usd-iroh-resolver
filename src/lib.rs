use bbl_usd::{ar, cpp, ffi, tf};
use ctor::ctor;
use futures_util::stream::{Stream, StreamExt};
use iroh::rpc_protocol::DownloadProgress;
use lazy_static::lazy_static;
use std::str::FromStr;

lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
    static ref IROH: Iroh =
        RUNTIME.block_on(iroh::client::quic::connect(4919)).unwrap();
}

use iroh::{
    client::{quic::{Doc, Iroh}, LiveEvent},
    net::magic_endpoint::NodeAddr,
    rpc_protocol::{BlobFormat, Hash},
    sync::{store::Query, ContentStatus, NamespaceId},
    ticket::{BlobTicket, DocTicket},
};

#[derive(Debug)]
enum HashOrTicket<'a> {
    Hash(Hash, &'a str),
    Ticket(NodeAddr, Hash),
    Doc(NamespaceId, Vec<u8>),
    DocTicket(DocTicket, Vec<u8>),
}

impl<'a> HashOrTicket<'a> {
    fn parse(string: &'a str) -> anyhow::Result<Self> {
        let url = url::Url::parse(string)?;

        Ok(match url.host_str() {
            Some("blob") => {
                let (_, path) = string.rsplit_once('/').unwrap();
                let (hash, ext) = path.split_once('.').unwrap();
                Self::Hash(iroh::rpc_protocol::Hash::from_str(hash)?, ext)
            }
            Some("ticket") => {
                let (_, path) = string.rsplit_once('/').unwrap();
                let (ticket, _ext) = path.split_once('.').unwrap();
                let ticket = iroh::ticket::BlobTicket::from_str(ticket)?;
                let (node, hash, format) = ticket.into_parts();
                if format != BlobFormat::Raw {
                    return Err(anyhow::anyhow!("{:?}", format));
                }
                Self::Ticket(node, hash)
            }
            Some("doc") => {
                let (rest, key_with_ext) = string.rsplit_once('/').unwrap();
                let (_, namespace) = rest.rsplit_once('/').unwrap();
                let (base32_encoded, _ext) = key_with_ext.split_once('.').unwrap();
                let namespace = iroh::sync::NamespaceId::from_str(namespace)?;
                Self::Doc(namespace, iroh::base32::parse_vec(base32_encoded)?)
            }
            Some("docticket") => {
                let (rest, key_with_ext) = string.rsplit_once('/').unwrap();
                let (_, ticket) = rest.rsplit_once('/').unwrap();
                let (base32_encoded, _ext) = key_with_ext.split_once('.').unwrap();
                let ticket = DocTicket::from_str(ticket)?;
                Self::DocTicket(ticket, iroh::base32::parse_vec(base32_encoded)?)
            }
            other => {
                return Err(anyhow::anyhow!("{:?}", other));
            }
        })
    }
}

async fn finish_download(
    mut stream: impl Stream<Item = anyhow::Result<DownloadProgress>> + std::marker::Unpin,
) -> anyhow::Result<()> {
    while let Some(item) = stream.next().await {
        match item? {
            DownloadProgress::AllDone => break,
            DownloadProgress::Abort(error) => {
                return Err(error.into());
            }
            _ => {}
        }
    }

    Ok(())
}

async fn wait_until_available(
    doc: &Doc,
    key: &[u8],
    iroh: &Iroh,
    mut looking_for_hash: Option<Hash>,
) -> anyhow::Result<bytes::Bytes> {
    let mut events = doc.subscribe().await?;

    while let Some(event) = events.next().await {
        match event? {
            // If a new insert was made with the key then just use that.
            LiveEvent::InsertLocal { entry }
            | LiveEvent::InsertRemote {
                entry,
                content_status: ContentStatus::Complete,
                from: _,
            } => {
                if entry.key() == key {
                    return iroh.blobs.read_to_bytes(entry.content_hash()).await;
                }
            }
            // If a new insert was made and we don't have the content available then at least we know what hash
            // we're looking for.
            LiveEvent::InsertRemote {
                entry,
                content_status: ContentStatus::Incomplete | ContentStatus::Missing,
                from: _,
            } => {
                if entry.key() == key {
                    looking_for_hash = Some(entry.content_hash());
                }
            }
            LiveEvent::ContentReady { hash } => {
                if looking_for_hash == Some(hash) {
                    return iroh.blobs.read_to_bytes(hash).await;
                }
            }
            LiveEvent::NeighborUp(_) | LiveEvent::NeighborDown(_) | LiveEvent::SyncFinished(_) => {}
        }
    }

    Err(anyhow::anyhow!("Event stream ran out."))
}

async fn handle_url(url: &str, iroh: &Iroh) -> anyhow::Result<bytes::Bytes> {
    match HashOrTicket::parse(url)? {
        HashOrTicket::Hash(hash, _ext) => {
            iroh.blobs.read_to_bytes(hash).await
        }
        HashOrTicket::Ticket(peer, hash) => {
            if let Ok(bytes) = iroh.blobs.read_to_bytes(hash).await {
                return Ok(bytes);
            }

            let download_stream = iroh.blobs.download(
                iroh::rpc_protocol::BlobDownloadRequest {
                    hash,
                    peer,
                    format: BlobFormat::Raw,
                    tag: iroh::rpc_protocol::SetTagOption::Auto,
                    out: iroh::rpc_protocol::DownloadLocation::Internal,
                },
            ).await?;

            finish_download(download_stream).await?;

            iroh.blobs.read_to_bytes(hash).await
        }
        HashOrTicket::Doc(namespace, key) => {
            let doc = iroh.docs.open(namespace).await?.unwrap();

            let entry = doc.get_one(Query::single_latest_per_key().key_exact(key)).await?
                .unwrap();

            entry.content_bytes(&*iroh).await
        }
        HashOrTicket::DocTicket(ticket, key) => {
            let doc = iroh.docs.import(ticket).await?;

            if let Some(entry) = doc
                .get_one(Query::single_latest_per_key().key_exact(&key))
                .await?
            {
                // If the content is available then we can just return it immediately
                if let Ok(bytes) = entry.content_bytes(&*iroh).await {
                    Ok(bytes)
                } else {
                    // Otherwise we need to wait until it becomes availble, but at least
                    // we know the hash we're waiting for.
                    wait_until_available(&doc, &key, iroh, Some(entry.content_hash())).await
                }
            } else {
                // We don't know the hash we're waiting for, so just wait for an insert to be made to the key.
                wait_until_available(&doc, &key, iroh, None).await
            }
        }
    }
}

extern "C" fn open_asset(
    path: *const ffi::ar_ResolvedPath_t,
    output: *mut *mut ffi::ar_AssetSharedPtr_t,
) {
    let string = ar::ResolvedPath::from_raw(path).get_path_string();
    match RUNTIME.block_on(handle_url(string.as_str(), &IROH)) {
        Ok(bytes) => unsafe {
            ffi::ar_asset_from_bytes(bytes.as_ptr() as _, bytes.len(), output);
        },
        Err(error) => {
            println!("{}", error);
        }
    }
}

extern "C" fn resolve(string: *const ffi::std_String_t, output: *mut *mut ffi::ar_ResolvedPath_t) {
    let string = cpp::StringRef::from_ptr(string);
    unsafe { *output = ar::ResolvedPathRef::new(&string).ptr() as _ };
}

extern "C" fn create_identifier(
    string: *const ffi::std_String_t,
    anchor: *const ffi::ar_ResolvedPath_t,
    output: *mut *mut ffi::std_String_t,
) {
    let anchor = ar::ResolvedPath::from_raw(anchor).get_path_string();
    let anchor = anchor.as_str();
    let string = cpp::StringRef::from_ptr(string);

    dbg!(string.as_str());

    // If the anchor is empty then just use the uri as-is.
    if anchor.is_empty() {
        unsafe {
            *output = string.ptr() as *mut ffi::std_String_t;
        }
        return;
    }

    // If the path is just a hash but the anchor is a ticket, construct a new ticket using
    // the peer from the anchor ticket.

    let (hash, ext, node) = match (
        HashOrTicket::parse(string.as_str()).unwrap(),
        HashOrTicket::parse(anchor).unwrap(),
    ) {
        (HashOrTicket::Hash(hash, ext), HashOrTicket::Ticket(node, ..)) => (hash, ext, node),
        _ => {
            unsafe {
                *output = string.ptr() as *mut ffi::std_String_t;
            }
            return;
        }
    };

    let ticket = BlobTicket::new(node, hash, iroh::rpc_protocol::BlobFormat::Raw).unwrap();

    let new_url = format!("iroh://ticket/{}.{}", ticket, ext);
    let new_url = std::ffi::CString::new(new_url).unwrap();
    let new_url = cpp::String::new(&new_url);

    unsafe {
        *output = new_url.ptr() as _;
    }
}

#[ctor]
fn ctor() {
    let ty = tf::Type::declare("IrohResolver");
    ty.set_factory(create_identifier, open_asset, resolve, None);
}
