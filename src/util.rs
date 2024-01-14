use futures_util::stream::{Stream, StreamExt};

use iroh::{
    client::{
        quic::{Doc, Iroh},
        Entry, LiveEvent,
    },
    rpc_protocol::{DownloadProgress, Hash},
    sync::{store::Query, ContentStatus},
};

pub async fn finish_download(
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

pub async fn wait_until_available(
    mut events: impl Stream<Item = anyhow::Result<LiveEvent>> + std::marker::Unpin,
    key: &[u8],
    iroh: &Iroh,
    mut looking_for_hash: Option<Hash>,
) -> anyhow::Result<bytes::Bytes> {
    while let Some(event) = events.next().await {
        //dbg!(looking_for_hash.as_ref().map(|h| h.to_string()), &event);

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

pub async fn get_entry_in_doc(doc: &Doc, key: &[u8]) -> anyhow::Result<Entry> {
    // Create the event stream early so we don't miss items.
    let mut events = doc.subscribe().await?;

    if let Some(entry) = doc
        .get_one(Query::single_latest_per_key().key_exact(key))
        .await?
    {
        Ok(entry)
    } else {
        while let Some(event) = events.next().await {
            match event? {
                LiveEvent::InsertLocal { entry } | LiveEvent::InsertRemote { entry, .. } => {
                    if entry.key() == key {
                        return Ok(entry);
                    }
                }
                LiveEvent::ContentReady { .. }
                | LiveEvent::NeighborUp(_)
                | LiveEvent::NeighborDown(_)
                | LiveEvent::SyncFinished(_) => {}
            }
        }

        Err(anyhow::anyhow!("Event stream ran out."))
    }
}

pub async fn print_keys(doc: &Doc) -> anyhow::Result<()> {
    let mut stream = doc.get_many(Query::all()).await?;

    while let Some(entry) = stream.next().await {
        dbg!(&entry);
    }

    Ok(())
}

pub async fn get_key_in_doc(doc: &Doc, iroh: &Iroh, key: &[u8]) -> anyhow::Result<bytes::Bytes> {
    // Create the event stream early so we don't miss items.
    let events = doc.subscribe().await?;

    if let Some(entry) = doc
        .get_one(Query::single_latest_per_key().key_exact(key))
        .await?
    {
        // If the content is available then we can just return it immediately
        match entry.content_bytes(iroh).await {
            Ok(bytes) => Ok(bytes),
            Err(_error) => {
                //dbg!(&_error, std::str::from_utf8(key));
                //dbg!(&entry);
                // Otherwise we need to wait until it becomes availble, but at least
                // we know the hash we're waiting for.
                wait_until_available(events, key, iroh, Some(entry.content_hash())).await
            }
        }
    } else {
        // We don't know the hash we're waiting for, so just wait for an insert to be made to the key.
        wait_until_available(events, key, iroh, None).await
    }
}
