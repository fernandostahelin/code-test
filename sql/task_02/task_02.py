import logging
import os
import sys

import psycopg2
from dotenv import load_dotenv


def setup_logging():
    """Configure logging with file and stream handlers"""
    logger = logging.getLogger(__name__)

    # File handler setup
    fh = logging.FileHandler("task_02.log")
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.setLevel(logging.DEBUG)

    # Stream handler setup
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


logger = setup_logging()


# Load environment variables
load_dotenv()


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


def get_existing_products(cursor):
    """Get existing products from target database"""
    cursor.execute(
        "SELECT product_id, category_id, quantity_per_unit, unit_price, units_in_stock, discontinued FROM target_products"
    )
    return {row[0]: row[1:] for row in cursor.fetchall()}


def get_source_products(cursor):
    """Get products from source database"""
    cursor.execute("""
        SELECT 
            product_id, product_name, category_id, 
            quantity_per_unit, unit_price, units_in_stock,
            units_on_order, discontinued
        FROM products
    """)
    return cursor.fetchall()


def insert_product(cursor, product, modified_by):
    """Insert a single product into target database"""
    insert_values = (
        product[0],
        product[1],
        product[2],
        product[3],
        product[4],
        product[5],
        product[6],
        product[7],
        modified_by,
        modified_by,
    )

    cursor.execute(
        """
        INSERT INTO target_products (
            product_id, product_name, category_id, 
            quantity_per_unit, unit_price, units_in_stock, 
            units_in_order, discontinued, created_by, 
            modified_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        insert_values,
    )


def sync_products(source_conn, target_conn, modified_by):
    """Sync products from source to target"""
    try:
        with source_conn.cursor() as source_cur, target_conn.cursor() as target_cur:
            # Get source and target data
            source_products = get_source_products(source_cur)
            logger.debug(f"Found {len(source_products)} products in source database")

            # Clear target table and insert new data
            truncate_target_table(target_cur, "target_products")

            # Insert products
            for product in source_products:
                insert_product(target_cur, product, modified_by)

            target_conn.commit()
            logger.info(
                f"Products sync completed. {len(source_products)} products synced"
            )

    except Exception as e:
        logger.error(f"Error syncing products: {str(e)}")
        target_conn.rollback()
        raise


def get_last_sync_timestamp(cursor):
    """Get the last sync timestamp for employees"""
    cursor.execute(
        "SELECT COALESCE(MAX(modified_at), '1900-01-01'::timestamp) FROM target_employees"
    )
    return cursor.fetchone()[0]


def get_modified_employees(cursor, last_sync):
    """Get modified employees since last sync"""
    cursor.execute("SELECT * FROM employees WHERE modified_at > %s", (last_sync,))
    return cursor.fetchall()


def get_source_employees(cursor):
    """Get all employees from source database"""
    cursor.execute("""
        SELECT employee_id, last_name, first_name,
               title, address, city, postal_code, country, reports_to
        FROM employees
    """)
    return cursor.fetchall()


def get_existing_employees(cursor):
    """Get existing employees from target database"""
    cursor.execute("""
        SELECT employee_id, last_name, first_name,
               title, address, city, postal_code, country, reports_to
        FROM target_employees
    """)
    return {row[0]: row[1:] for row in cursor.fetchall()}


def update_employee(cursor, employee, modified_by):
    """Update existing employee record"""
    cursor.execute(
        """
        UPDATE target_employees 
        SET last_name = %s, first_name = %s, middle_name = %s,
            title = %s, address = %s, city = %s, 
            postal_code = %s, country = %s, reports_to = %s,
            modified_by = %s, modified_at = CURRENT_TIMESTAMP
        WHERE employee_id = %s
        """,
        (*employee[1:], modified_by, employee[0]),
    )


def insert_employee(cursor, employee, modified_by):
    """Insert new employee record"""
    cursor.execute(
        """
        INSERT INTO target_employees (
            employee_id, last_name, first_name,
            title, address, city, postal_code, country, 
            reports_to, created_by, created_at, 
            modified_by, modified_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                 CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP)
        """,
        (*employee, modified_by, modified_by),
    )


def truncate_target_table(cursor, table_name):
    """Truncate the specified target table"""
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    logger.info(f"Cleared {table_name} table")


def sync_employees(source_conn, target_conn, modified_by):
    """Sync employees from source to target in two passes to handle foreign key constraints"""
    try:
        with source_conn.cursor() as source_cur, target_conn.cursor() as target_cur:
            # Get all source employees
            source_employees = get_source_employees(source_cur)

            # First pass: Insert all employees with reports_to set to NULL
            for employee in source_employees:
                employee_id = employee[0]
                # Create a modified employee tuple with reports_to set to NULL
                modified_employee = employee[:-1] + (
                    None,
                )  # Remove original reports_to and add NULL
                insert_employee(target_cur, modified_employee, modified_by)
                logger.debug(f"Inserted employee {employee_id} (first pass)")

            # Second pass: Update reports_to relationships
            for employee in source_employees:
                employee_id = employee[0]
                reports_to = employee[-1]  # Get the original reports_to value
                if reports_to is not None:
                    target_cur.execute(
                        """
                        UPDATE target_employees 
                        SET reports_to = %s,
                            modified_by = %s,
                            modified_at = CURRENT_TIMESTAMP
                        WHERE employee_id = %s
                    """,
                        (reports_to, modified_by, employee_id),
                    )
                    logger.debug(f"Updated reports_to for employee {employee_id}")

            target_conn.commit()
            logger.info(
                f"Employees sync completed. {len(source_employees)} employees processed"
            )

    except Exception as e:
        logger.error(f"Error syncing employees: {str(e)}")
        target_conn.rollback()
        raise


def main():
    """Main function to orchestrate the data sync process"""
    modified_by = "SYSTEM"  # You might want to make this configurable

    try:
        # Initialize connections
        source_conn = get_db_connection("source_db")
        target_conn = get_db_connection("target_db")

        logger.info("Starting data sync process")

        # Sync products
        sync_products(source_conn, target_conn, modified_by)

        # Sync employees
        sync_employees(source_conn, target_conn, modified_by)

        logger.info("Data sync process completed successfully")

    except Exception as e:
        logger.error(f"Data sync process failed: {str(e)}")
        raise

    finally:
        # Close connections
        for conn in [source_conn, target_conn]:
            if conn:
                conn.close()
                logger.debug("Database connection closed")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    try:
        main()
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        sys.exit(1)
