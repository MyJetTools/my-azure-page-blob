use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::MyPageBlob;

pub async fn write_pages<TMyPageBlob: MyPageBlob>(
    page_blob: &TMyPageBlob,
    start_page_no: usize,
    max_pages_to_write_per_round_trip: usize,
    payload: Vec<u8>,
) -> Result<(), AzureStorageError> {
    let mut remain_pages_to_write = payload.len() / BLOB_PAGE_SIZE;
    if remain_pages_to_write <= max_pages_to_write_per_round_trip {
        page_blob.save_pages(start_page_no, payload).await?;
        return Ok(());
    }
    let mut start_page_no = start_page_no;
    let mut buffer_offset = 0;

    while remain_pages_to_write >= max_pages_to_write_per_round_trip {
        let buffer_to_send = &payload
            [buffer_offset..buffer_offset + max_pages_to_write_per_round_trip * BLOB_PAGE_SIZE];
        page_blob
            .save_pages(start_page_no, buffer_to_send.to_vec())
            .await?;

        start_page_no += max_pages_to_write_per_round_trip;
        buffer_offset += max_pages_to_write_per_round_trip * BLOB_PAGE_SIZE;
        remain_pages_to_write -= max_pages_to_write_per_round_trip;
    }

    if remain_pages_to_write > 0 {
        let buffer_to_send = &payload
            [buffer_offset..buffer_offset + max_pages_to_write_per_round_trip * BLOB_PAGE_SIZE];

        page_blob
            .save_pages(start_page_no, buffer_to_send.to_vec())
            .await?;
    }

    Ok(())
}
