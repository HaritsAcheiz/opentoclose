import logging
from fetch_properties import execute_fetch_properties
from fetch_agents import execute_fetch_agents
from close_paid_data import execute_read_parquet_and_create_google_sheets


def run_pipeline():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting the pipeline")

        # # Step 1: Execute fetch_properties
        logger.info("Executing fetch_properties")
        # execute_fetch_properties()
        logger.info("fetch_properties completed successfully")

        # # Step 2: Execute read_parquet_and_create_google_sheet
        logger.info("Executing read_parquet_and_create_google_sheet")
        execute_read_parquet_and_create_google_sheets()
        logger.info("read_parquet_and_create_google_sheet completed successfully")

        # Step 3: Execute fetch_agents
        # logger.info("Executing fetch_agents")
        # execute_fetch_agents()
        # logger.info("fetch_agents completed successfully")

        # Step 4: Execute read_parquet_and_create_google_sheet
        # logger.info("Executing read_parquet_and_create_google_sheet")
        # execute_read_parquet_and_create_google_sheets()
        # logger.info("read_parquet_and_create_google_sheet completed successfully")

        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"An error occurred during the pipeline execution: {str(e)}")
        raise


if __name__ == "__main__":
    run_pipeline()
