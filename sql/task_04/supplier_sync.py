import logging
import os
import sys
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv


def setup_logging():
    """Configure logging with file and stream handlers"""
    logger = logging.getLogger(__name__)

    # Clear any existing handlers
    logger.handlers = []

    # Set base logging level
    logger.setLevel(logging.DEBUG)

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # File handler setup (detailed logging)
    fh = logging.FileHandler("supplier_sync.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(detailed_formatter)
    logger.addHandler(fh)

    # Stream handler setup (less detailed for console)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(console_formatter)
    logger.addHandler(sh)

    return logger


logger = setup_logging()


def get_db_config(db_name):
    """Get database configuration based on database name"""
    if db_name == "source_db":
        return {
            "database": os.getenv("POSTGRES_DB_SOURCE"),
            "host": os.getenv("POSTGRES_SOURCE_HOST"),
            "user": os.getenv("POSTGRES_USER_SOURCE"),
            "password": os.getenv("POSTGRES_PASS_SOURCE"),
            "port": os.getenv("POSTGRES_SOURCE_PORT"),
        }
    elif db_name == "target_db":
        return {
            "database": os.getenv("POSTGRES_DB_ANALYTICS"),
            "host": os.getenv("POSTGRES_ANALYTICS_HOST"),
            "user": os.getenv("POSTGRES_USER_ANALYTICS"),
            "password": os.getenv("POSTGRES_PASSWORD_ANALYTICS"),
            "port": os.getenv("POSTGRES_ANALYTICS_PORT"),
        }


def get_db_connection(db_name):
    """Create database connection based on database name"""
    config = get_db_config(db_name)
    try:
        conn = psycopg2.connect(**config)
        logger.info(f"Successfully connected to database: {db_name}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database {db_name}: {str(e)}")
        raise


def get_tracked_columns():
    """Return list of columns that need change tracking"""
    return [
        "contact_name",
        "contact_title",
        "address",
        "city",
        "region",
        "postal_code",
        "country",
        "phone",
    ]


def get_source_suppliers(cursor):
    """Get all suppliers from source database"""
    cursor.execute("""
        SELECT supplier_id, company_name, contact_name, contact_title,
               address, city, region, postal_code, country, 
               phone, fax, homepage
        FROM suppliers
    """)
    return cursor.fetchall()


def get_current_suppliers(cursor):
    """Get current supplier records from dimension table"""
    cursor.execute("""
        SELECT supplier_id, company_name, contact_name, contact_title,
               address, city, region, postal_code, country, 
               phone, fax, homepage
        FROM supplier_dimension
        WHERE is_current = true
    """)
    return {row[0]: row[1:] for row in cursor.fetchall()}


def detect_changes(source_row, target_row, tracked_columns):
    """Compare source and target rows to detect changes in tracked columns"""
    if not target_row:
        logger.debug("New supplier detected (no existing record)")
        return True

    source_values = source_row[2:]  # Skip supplier_id and company_name
    target_values = target_row[1:]  # Skip company_name

    for i, (source_val, target_val) in enumerate(zip(source_values, target_values)):
        if i < len(tracked_columns):
            logger.debug(
                f"Comparing {tracked_columns[i]}: source='{source_val}' target='{target_val}'"
            )
            if source_val != target_val:
                logger.debug(f"Change detected in {tracked_columns[i]}")
                return True
    return False


def sync_supplier_dimension(source_conn, target_conn, modified_by):
    """Sync suppliers from source to target using SCD Type 2"""
    logger = logging.getLogger(__name__)
    tracked_columns = get_tracked_columns()
    current_timestamp = datetime.now(timezone.utc)

    try:
        with source_conn.cursor() as source_cur, target_conn.cursor() as target_cur:
            logger.info("Starting supplier dimension sync process")

            # Get source and current target data
            logger.debug("Fetching suppliers from source database")
            source_suppliers = get_source_suppliers(source_cur)
            logger.info(f"Found {len(source_suppliers)} suppliers in source database")

            logger.debug("Fetching current supplier records from target database")
            current_suppliers = get_current_suppliers(target_cur)
            logger.info(
                f"Found {len(current_suppliers)} current supplier records in target database"
            )

            # Tracking metrics
            updates = 0
            new_records = 0
            unchanged = 0

            for supplier in source_suppliers:
                supplier_id = supplier[0]
                current_record = current_suppliers.get(supplier_id)

                logger.debug(f"Processing supplier_id: {supplier_id}")

                if detect_changes(supplier, current_record, tracked_columns):
                    if current_record:
                        logger.debug(
                            f"Changes detected for supplier_id: {supplier_id}, expiring current record"
                        )
                        target_cur.execute(
                            """
                            UPDATE supplier_dimension
                            SET end_date = %s,
                                is_current = false,
                                modified_by = %s,
                                modified_at = %s
                            WHERE supplier_id = %s AND is_current = true
                            """,
                            (
                                current_timestamp,
                                modified_by,
                                current_timestamp,
                                supplier_id,
                            ),
                        )
                        updates += 1
                    else:
                        logger.debug(f"New supplier detected: {supplier_id}")
                        new_records += 1

                    # Insert new record
                    logger.debug(f"Inserting new record for supplier_id: {supplier_id}")
                    target_cur.execute(
                        """
                        INSERT INTO supplier_dimension (
                            supplier_id, company_name, contact_name, contact_title,
                            address, city, region, postal_code, country,
                            phone, fax, homepage, effective_date, end_date,
                            is_current, created_by, modified_by
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, NULL, true, %s, %s
                        )
                        """,
                        (*supplier, current_timestamp, modified_by, modified_by),
                    )
                else:
                    logger.debug(f"No changes detected for supplier_id: {supplier_id}")
                    unchanged += 1

            target_conn.commit()
            logger.info("Supplier dimension sync completed successfully")
            logger.info(
                f"Summary: {new_records} new suppliers, {updates} updates, {unchanged} unchanged"
            )

    except Exception as e:
        logger.error(f"Error syncing supplier dimension: {str(e)}", exc_info=True)
        target_conn.rollback()
        raise


def main():
    """Main function to orchestrate the supplier dimension sync"""
    load_dotenv()
    logger = setup_logging()
    modified_by = "SYSTEM"

    logger.info("=== Starting Supplier Dimension Sync Process ===")
    start_time = datetime.now()

    try:
        logger.info("Establishing database connections")
        source_conn = get_db_connection("source_db")
        target_conn = get_db_connection("target_db")

        sync_supplier_dimension(source_conn, target_conn, modified_by)

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Process completed successfully in {duration} ===")

    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.error(f"Process failed after running for {duration}")
        logger.error(f"Exception details:{e}", exc_info=True)
        raise

    finally:
        logger.info("Closing database connections")
        for conn in [source_conn, target_conn]:
            if conn:
                conn.close()
                logger.debug("Database connection closed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        sys.exit(1)
