"""
Sync State Management Service

This module provides functionality to:
1. Check if incremental sync is enabled via Parameter Store
2. Retrieve and update sync state from DynamoDB
3. Determine date ranges for incremental or full sync
"""

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any, List
import boto3
from botocore.exceptions import ClientError
import pandas as pd

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


class SyncStateManager:
    """Manages sync state for HubSpot data ingestion with incremental sync support."""

    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb")
        self.ssm = boto3.client("ssm")
        self.table_name = os.environ.get("SYNC_STATE_TABLE")
        self.parameter_name = os.environ.get("INCREMENTAL_SYNC_PARAMETER")
        self.start_date_fallback = os.environ.get("START_DATE", "2025-01-01")

        if not self.table_name:
            raise RuntimeError("SYNC_STATE_TABLE environment variable not set")
        if not self.parameter_name:
            raise RuntimeError(
                "INCREMENTAL_SYNC_PARAMETER environment variable not set"
            )

        self.table = self.dynamodb.Table(self.table_name)

    def is_incremental_sync_enabled(self) -> bool:
        """Check if incremental sync is enabled via Parameter Store."""
        try:
            response = self.ssm.get_parameter(Name=self.parameter_name)
            value = response["Parameter"]["Value"].lower()
            return value in ("true", "1", "yes", "enabled")
        except ClientError as e:
            LOG.warning(f"Failed to read parameter {self.parameter_name}: {e}")
            return False

    def get_sync_state(self, object_type: str) -> Optional[Dict[str, Any]]:
        """Retrieve sync state for a specific object type."""
        try:
            response = self.table.get_item(Key={"object_type": object_type})
            if "Item" in response:
                return response["Item"]
            return None
        except ClientError as e:
            LOG.warning(f"Failed to get sync state for {object_type}: {e}")
            return None

    def update_sync_state(
            self,
            object_type: str,
            last_created_at: Optional[str] = None,
            last_modified_at: Optional[str] = None,
            records_processed: int = 0,
    ) -> None:
        """Update sync state for a specific object type."""
        try:
            item = {
                "object_type": object_type,
                "last_sync_at": datetime.now(timezone.utc).isoformat(),
                "records_processed": records_processed,
            }

            if last_created_at:
                item["last_created_at"] = last_created_at
            if last_modified_at:
                item["last_modified_at"] = last_modified_at

            self.table.put_item(Item=item)
            LOG.info(f"Updated sync state for {object_type}")
        except ClientError as e:
            LOG.error(f"Failed to update sync state for {object_type}: {e}")
            raise

    def get_sync_dates(
            self, object_type: str, buffer_hours: int = 2
    ) -> Tuple[Optional[str], str]:
        """
        Determine the date range for sync based on incremental sync settings.

        Args:
            object_type: The HubSpot object type (deals, contacts, etc.)
            buffer_hours: Hours to overlap with last sync to catch late updates

        Returns:
            Tuple of (from_date, to_date) where from_date is None for full sync
        """
        to_date = datetime.now(timezone.utc).isoformat()

        if not self.is_incremental_sync_enabled():
            LOG.info(
                f"Incremental sync disabled for {object_type}, performing full sync"
            )
            return None, to_date

        sync_state = self.get_sync_state(object_type)
        if not sync_state:
            LOG.info(
                f"No sync state found for {object_type}, performing initial full sync"
            )
            return None, to_date

        # Use the most recent date from either created_at or modified_at
        last_created = sync_state.get("last_created_at")
        last_modified = sync_state.get("last_modified_at")

        # Choose the more recent date, or fall back to last_sync_at
        from_date_str = None
        if last_created and last_modified:
            # Use the more recent of the two
            created_dt = pd.to_datetime(last_created, utc=True)
            modified_dt = pd.to_datetime(last_modified, utc=True)
            from_date_str = max(created_dt, modified_dt).isoformat()
        elif last_created:
            from_date_str = last_created
        elif last_modified:
            from_date_str = last_modified
        else:
            # Fall back to last sync time
            from_date_str = sync_state.get("last_sync_at")

        if from_date_str:
            # Apply buffer to catch any late updates
            from_dt = pd.to_datetime(from_date_str, utc=True) - timedelta(
                hours=buffer_hours
            )
            from_date_iso = from_dt.isoformat()
            LOG.info(
                f"Incremental sync for {object_type} from {from_date_iso} (with {buffer_hours}h buffer)"
            )
            return from_date_iso, to_date
        else:
            LOG.info(
                f"No valid date found in sync state for {object_type}, performing full sync"
            )
            return None, to_date

    def extract_date_bounds_from_data(
            self, df: pd.DataFrame
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract the min and max dates from a DataFrame to update sync state.

        Args:
            df: DataFrame with 'created_at' and 'last_modified_at' columns

        Returns:
            Tuple of (max_created_at, max_last_modified_at) as ISO strings
        """
        if df.empty:
            return None, None

        max_created = None
        max_modified = None

        if "created_at" in df.columns:
            created_series = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
            if not created_series.isna().all():
                max_created = created_series.max().isoformat()

        if "last_modified_at" in df.columns:
            modified_series = pd.to_datetime(
                df["last_modified_at"], utc=True, errors="coerce"
            )
            if not modified_series.isna().all():
                max_modified = modified_series.max().isoformat()

        return max_created, max_modified

    def write_with_merge_strategy(
            self,
            df: pd.DataFrame,
            s3_path: str,
            partition_cols: List[str],
            primary_key_col: str,
            compression: str = "snappy",
    ) -> None:
        """
        Write DataFrame to S3 using appropriate strategy based on incremental sync setting.

        For incremental sync: merges with existing partition data to avoid duplicates.
        For full sync: overwrites all data.

        Args:
            df: DataFrame to write
            s3_path: S3 path (e.g., "s3://bucket/curated/deals/")
            partition_cols: List of partition column names (e.g., ["dt"])
            primary_key_col: Column name for deduplication (e.g., "deal_id")
            compression: Compression format for parquet files
        """
        import awswrangler as wr

        if self.is_incremental_sync_enabled():
            # Get unique partitions from new data
            if len(partition_cols) == 1:
                partition_col = partition_cols[0]
                partitions_to_merge = df[partition_col].unique()
            else:
                # For multiple partition columns, get unique combinations
                partitions_to_merge = (
                    df[partition_cols].drop_duplicates().to_dict("records")
                )

            # Read existing data for these partitions
            existing_data = []

            if len(partition_cols) == 1:
                # Single partition column (most common case)
                for partition_value in partitions_to_merge:
                    partition_path = f"{s3_path}{partition_col}={partition_value}/"
                    try:
                        existing_df = wr.s3.read_parquet(
                            path=partition_path, dataset=False
                        )
                        if not existing_df.empty:
                            existing_data.append(existing_df)
                    except Exception:
                        # Partition doesn't exist yet, skip
                        pass
            else:
                # Multiple partition columns
                for partition_combo in partitions_to_merge:
                    partition_parts = [
                        f"{col}={partition_combo[col]}" for col in partition_cols
                    ]
                    partition_path = f"{s3_path}{'/'.join(partition_parts)}/"
                    try:
                        existing_df = wr.s3.read_parquet(
                            path=partition_path, dataset=False
                        )
                        if not existing_df.empty:
                            existing_data.append(existing_df)
                    except Exception:
                        # Partition doesn't exist yet, skip
                        pass

            # Combine existing and new data
            if existing_data:
                all_existing_df = pd.concat(existing_data, ignore_index=True)
                combined_df = pd.concat([all_existing_df, df], ignore_index=True)
                # Deduplicate by primary key, keeping the latest (most recent data)
                final_df = combined_df.drop_duplicates(
                    subset=[primary_key_col], keep="last"
                )
            else:
                final_df = df

            # Write merged data, overwriting the affected partitions
            wr.s3.to_parquet(
                df=final_df,
                path=s3_path,
                dataset=True,
                compression=compression,
                partition_cols=partition_cols,
                mode="overwrite_partitions",
            )
        else:
            # Full sync - simply overwrite everything
            wr.s3.to_parquet(
                df=df,
                path=s3_path,
                dataset=True,
                compression=compression,
                partition_cols=partition_cols,
                mode="overwrite_partitions",
            )


def get_sync_manager() -> SyncStateManager:
    """Get a configured SyncStateManager instance."""
    return SyncStateManager()
