import csv
import logging
import os
import sys
from datetime import datetime

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
    fh = logging.FileHandler("supplier_import.log")
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



def get_existing_supplier_ids(cursor):
    """Get set of existing supplier IDs"""
    cursor.execute("SELECT supplier_id FROM imported_supplier")
    return {row[0] for row in cursor.fetchall()}


def import_suppliers(conn, csv_path, modified_by):
    """Import suppliers from CSV file while preventing duplicates"""
    try:
        with conn.cursor() as cur:
            existing_ids = get_existing_supplier_ids(cur)

            # Tracking metrics
            total_records = 0
            skipped_records = 0
            imported_records = 0

            with open(csv_path, "r") as csvfile:
                reader = csv.DictReader(csvfile)

                for row in reader:
                    total_records += 1
                    supplier_id = int(row["supplier_id"])

                    if supplier_id in existing_ids:
                        logger.debug(f"Skipping existing supplier_id: {supplier_id}")
                        skipped_records += 1
                        continue

                    cur.execute(
                        """
                        INSERT INTO imported_supplier (
                            supplier_id, company_name, contact_name, contact_title,
                            address, city, region, postal_code, country,
                            phone, fax, homepage, created_by
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            supplier_id,
                            row["company_name"],
                            row["contact_name"],
                            row["contact_title"],
                            row["address"],
                            row["city"],
                            row["region"],
                            row["postal_code"],
                            row["country"],
                            row["phone"],
                            row["fax"],
                            row["homepage"],
                            modified_by,
                        ),
                    )
                    imported_records += 1
                    logger.debug(f"Imported supplier_id: {supplier_id}")

            conn.commit()
            logger.info(
                f"Import completed: {imported_records} imported, {skipped_records} skipped, {total_records} total"
            )

    except Exception as e:
        logger.error(f"Error importing suppliers: {str(e)}")
        conn.rollback()
        raise


def main():
    """Main function to orchestrate the supplier import process"""
    csv_path = os.path.join(os.path.dirname(__file__), "suppliers.csv")
    modified_by = "SYSTEM"

    logger.info("=== Starting Supplier Import Process ===")
    start_time = datetime.now()

    try:
        logger.info(f"Looking for CSV file at: {os.path.abspath(csv_path)}")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        conn = get_db_connection("target_db")
        import_suppliers(conn, csv_path, modified_by)

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"=== Process completed successfully in {duration} ===")

    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time
        logger.error(f"Process failed after running for {duration}")
        logger.error(f"Exception details: {str(e)}", exc_info=True)
        raise

    finally:
        if "conn" in locals() and conn:
            conn.close()
            logger.debug("Database connection closed")


if __name__ == "__main__":
    load_dotenv()
    logger = setup_logging()

    try:
        main()
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        sys.exit(1)
