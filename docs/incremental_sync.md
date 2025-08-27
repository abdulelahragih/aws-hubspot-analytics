# Incremental Sync Configuration

This document explains how to configure and use the incremental sync feature for HubSpot data ingestion.

## Overview

The incremental sync feature allows the system to only fetch data that has been created or modified since the last successful sync, rather than re-fetching all data every time. This significantly improves performance and reduces API usage.

## Configuration

### Enable/Disable Incremental Sync

Incremental sync is controlled by an AWS Systems Manager Parameter Store parameter:

- **Parameter Name**: `/hubspot-analytics/incremental-sync-enabled`
- **Allowed Values**:
  - `true`, `1`, `yes`, `enabled` - Enable incremental sync
  - `false`, `0`, `no`, `disabled` - Disable incremental sync (default)

### Setting the Parameter

You can update the parameter using AWS CLI:

```bash
# Enable incremental sync
aws ssm put-parameter \
  --name "/hubspot-analytics/incremental-sync-enabled" \
  --value "true" \
  --overwrite

# Disable incremental sync
aws ssm put-parameter \
  --name "/hubspot-analytics/incremental-sync-enabled" \
  --value "false" \
  --overwrite
```

Or through the AWS Console:

1. Go to AWS Systems Manager > Parameter Store
2. Find `/hubspot-analytics/incremental-sync-enabled`
3. Edit the value to `true` or `false`

## How It Works

### Sync State Tracking

The system tracks sync state in a DynamoDB table called `hubspot-sync-state` with the following structure:

- `object_type` (Partition Key): The type of HubSpot object (deals, contacts, activities, companies, contacts_dim)
- `last_sync_at`: Timestamp of when the sync completed
- `last_created_at`: Most recent `created_at` timestamp from the data
- `last_modified_at`: Most recent `last_modified_at` timestamp from the data
- `records_processed`: Number of records processed in the last sync

### Incremental Logic

When incremental sync is enabled:

1. The system checks the sync state for the object type
2. If no previous sync state exists, performs a full sync
3. If sync state exists, uses the more recent of `last_created_at` or `last_modified_at` as the starting point
4. Applies a 2-hour buffer (overlap) to ensure no data is missed
5. Fetches only records created/modified since that timestamp
6. **Uses merge strategy** to combine new data with existing partition data, avoiding duplicates
7. Updates the sync state with the latest timestamps from the processed data

### Write Strategy

The system uses different write strategies based on incremental sync settings:

- **Full Sync (incremental sync disabled)**: Uses `overwrite_partitions` mode to replace all existing data
- **Incremental Sync (incremental sync enabled)**: Uses a **merge strategy** via `sync_manager.write_with_merge_strategy()`:
  1. Reads existing data from affected date partitions
  2. Combines with new incremental data
  3. Deduplicates by primary key (deal_id, contact_id, etc.), keeping latest records
  4. Writes merged data back using `overwrite_partitions` for affected partitions only

This merge approach prevents both data loss and duplicate records while maintaining date-based partitioning.

### Code Architecture

The merge strategy is implemented as a reusable method in the `SyncStateManager` class:

```python
sync_manager.write_with_merge_strategy(
    df=dataframe,
    s3_path="s3://bucket/path/",
    partition_cols=["dt"],
    primary_key_col="deal_id"
)
```

This ensures consistent behavior across all data ingestion functions.

### Fallback Behavior

- If incremental sync is disabled, the system always performs a full sync
- If there's an error reading the parameter or sync state, it falls back to full sync
- If sync state update fails, it logs a warning but doesn't fail the entire operation

## Supported Object Types

Incremental sync is supported for the following HubSpot object types:

- **deals**: Uses `hs_lastmodifieddate` for incremental fetching
- **activities**: Uses `hs_lastmodifieddate` for incremental fetching
- **contacts**: Uses `lastmodifieddate` for incremental fetching
- **companies**: Uses `hs_lastmodifieddate` for incremental fetching
- **contacts_dim**: Uses `lastmodifieddate` for incremental fetching

## Monitoring

### Logs

Each function logs whether it's performing incremental or full sync:

- `"Performing incremental sync from {from_date} to {to_date}"` - Incremental sync
- `"Performing full sync"` - Full sync

### DynamoDB Table

You can check the sync state by querying the `hubspot-sync-state` DynamoDB table to see:

- When each object type was last synced
- How many records were processed
- The latest timestamps seen for each object type

## Important Considerations

### Duplicate Handling

The system uses a **merge strategy** for incremental sync that automatically handles duplicates:

- **During Processing**: Reads existing partition data and merges with new data
- **Deduplication**: Uses primary keys (deal_id, contact_id, company_id, activity_id) to deduplicate
- **Conflict Resolution**: Keeps the latest version of each record (`keep="last"`)
- **Result**: Clean, deduplicated data in each partition

This eliminates the need for complex query-time deduplication in most cases.

### Partition Strategy

The current partitioning strategy uses `dt` (date) columns:

- **Deals/Activities**: Partitioned by `created_at` date
- **Companies/Contacts**: Partitioned by `last_modified_at` or `created_at` date

This means records from the same day will be in the same partition, regardless of the specific time.

## Best Practices

1. **Initial Setup**: Start with incremental sync disabled for the first run to establish baseline sync state
2. **Monitoring**: Monitor the sync state table to ensure incremental sync is working as expected
3. **Buffer Time**: The 2-hour buffer helps catch late updates but can be adjusted in the code if needed
4. **Recovery**: If data seems inconsistent, disable incremental sync temporarily to force a full refresh
5. **Query Design**: Design your analytics queries to handle potential duplicates gracefully
6. **Partition Pruning**: Take advantage of date partitioning in your queries for better performance

## Troubleshooting

### Force Full Sync

To force a full sync for a specific object type:

1. Disable incremental sync globally, OR
2. Delete the sync state record for that object type from DynamoDB:

```bash
aws dynamodb delete-item \
  --table-name hubspot-sync-state \
  --key '{"object_type": {"S": "deals"}}'
```

### Reset All Sync State

To reset all sync state and start fresh:

```bash
aws dynamodb scan --table-name hubspot-sync-state --query "Items[].object_type.S" --output text | \
xargs -I {} aws dynamodb delete-item --table-name hubspot-sync-state --key '{"object_type": {"S": "{}"}}'
```
